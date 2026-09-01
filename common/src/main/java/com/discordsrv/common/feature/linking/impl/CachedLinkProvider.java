/*
 * This file is part of DiscordSRV, licensed under the GPLv3 License
 * Copyright (c) 2016-2026 Austin "Scarsz" Shapiro, Henri "Vankka" Schubin and DiscordSRV contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.discordsrv.common.feature.linking.impl;

import com.discordsrv.api.eventbus.EventPriorities;
import com.discordsrv.api.eventbus.Subscribe;
import com.discordsrv.api.task.Task;
import com.discordsrv.common.DiscordSRV;
import com.discordsrv.common.core.logging.Logger;
import com.discordsrv.common.events.player.PlayerConnectedEvent;
import com.discordsrv.common.feature.linking.AccountLink;
import com.discordsrv.common.feature.linking.LinkProvider;
import com.discordsrv.common.feature.linking.LinkStore;
import com.github.benmanes.caffeine.cache.*;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;

public abstract class CachedLinkProvider implements LinkProvider {

    private static final AccountLink UNLINKED = new AccountLink(new UUID(0, 0), -1L, LocalDateTime.now(), LocalDateTime.now());

    protected final DiscordSRV discordSRV;
    protected final Logger logger;
    private final Cache<Long, AccountLink> userToPlayer;
    private final AsyncLoadingCache<UUID, AccountLink> playerToUser;
    private final Set<UUID> linkingAllowed = new CopyOnWriteArraySet<>();

    public CachedLinkProvider(DiscordSRV discordSRV, Logger logger) {
        this.discordSRV = discordSRV;
        this.logger = logger;

        this.userToPlayer = discordSRV.caffeineBuilder().build();
        this.playerToUser = discordSRV.caffeineBuilder()
                .refreshAfterWrite(Duration.ofMinutes(5))
                .removalListener((RemovalListener<UUID, AccountLink>) (key, value, cause) -> {
                    logger.trace("Removing " + key + " from cache (" + cause + ")");
                    if (value != null && cause != RemovalCause.REPLACED) {
                        userToPlayer.invalidate(value.userId());
                    }
                })
                .buildAsync(new AsyncCacheLoader<>() {
                    @Override
                    public @NotNull CompletableFuture<AccountLink> asyncLoad(@NotNull UUID key, @NotNull Executor executor) {
                        logger.trace("Loading " + key + " into cache");
                        return query(key, linkingAllowed.remove(key))
                                .thenApply(optionalLink -> {
                                    if (optionalLink.isEmpty()) {
                                        return UNLINKED;
                                    }

                                    AccountLink link = optionalLink.get();
                                    userToPlayer.put(link.userId(), link);
                                    return link;
                                })
                                .getFuture();
                    }

                    @Override
                    public @NotNull CompletableFuture<AccountLink> asyncReload(
                            @NotNull UUID key,
                            @NotNull AccountLink oldValue,
                            @NotNull Executor executor
                    ) {
                        if (discordSRV.playerProvider().player(key) == null) {
                            // Don't keep players that aren't online in cache
                            return CompletableFuture.completedFuture(null);
                        }

                        // Player is still online
                        return asyncLoad(key, executor);
                    }
                });
        discordSRV.eventBus().subscribe(this);
    }

    @Override
    public Task<Optional<AccountLink>> get(@NotNull UUID playerUUID) {
        return Task.of(playerToUser.get(playerUUID).thenApply(value -> {
            if (value == UNLINKED) {
                return Optional.empty();
            }
            return Optional.of(value);
        }));
    }

    @Override
    public Optional<AccountLink> getCached(@NotNull UUID player) {
        AccountLink value = playerToUser.synchronous().getIfPresent(player);
        return Optional.ofNullable(value == null || value == UNLINKED ? null : value);
    }

    @Override
    public Task<Optional<AccountLink>> get(long userId) {
        AccountLink cached = userToPlayer.getIfPresent(userId);
        if (cached != null) {
            return Task.completed(cached == UNLINKED ? Optional.empty() : Optional.of(cached));
        }

        return query(userId).thenApply(optionalLink -> {
            if (optionalLink.isEmpty()) {
                userToPlayer.put(userId, UNLINKED);
                return optionalLink;
            }

            AccountLink link = optionalLink.get();
            userToPlayer.put(userId, link);
            playerToUser.put(link.playerUUID(), CompletableFuture.completedFuture(link));
            return optionalLink;
        });
    }

    @Override
    public Optional<AccountLink> getCached(long discordId) {
        AccountLink value = userToPlayer.getIfPresent(discordId);
        return Optional.ofNullable(value == null || value == UNLINKED ? null : value);
    }

    @Subscribe(priority = EventPriorities.PRE + 1)
    public void onPlayerConnected(PlayerConnectedEvent event) {
        // Cache logged in players
        UUID uuid = event.player().uniqueId();
        linkingAllowed.add(uuid);
        playerToUser.get(uuid);
    }

    protected void addToCache(AccountLink link) {
        playerToUser.put(link.playerUUID(), CompletableFuture.completedFuture(link));
    }

    protected void evictFromCache(UUID playerUUID) {
        playerToUser.synchronous().invalidate(playerUUID);
    }

    public static abstract class Store extends CachedLinkProvider implements LinkStore {

        public Store(DiscordSRV discordSRV, Logger logger) {
            super(discordSRV, logger);
        }

        public abstract Task<Void> link(@NotNull AccountLink link);
        public abstract Task<Void> unlink(@NotNull UUID playerUUID, long userId);

        @Override
        public final Task<Void> createLink(@NotNull AccountLink link) {
            return link(link).thenApply(v -> {
                addToCache(link);
                return null;
            });
        }

        @Override
        public Task<Void> removeLink(@NotNull UUID playerUUID, long userId) {
            return unlink(playerUUID, userId).thenApply(v -> {
                evictFromCache(playerUUID);
                return null;
            });
        }
    }
}

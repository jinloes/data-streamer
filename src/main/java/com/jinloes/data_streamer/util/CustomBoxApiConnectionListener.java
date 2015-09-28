package com.jinloes.data_streamer.util;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxAPIConnectionListener;
import com.box.sdk.BoxAPIException;
import io.vertx.core.json.JsonObject;

/**
 * Created by jinloes on 9/28/15.
 */
/*public class CustomBoxApiConnectionListener implements BoxAPIConnectionListener {
    @Override
    public void onRefresh(BoxAPIConnection api) {
        accessToken = api.getAccessToken();
        System.out.println("Access token refresh: " + api.getAccessToken());
        refreshToken = api.getRefreshToken();
        System.out.println("Refresh token refresh: " + api.getRefreshToken());
        mongoClient.update("oauth_tokens", new JsonObject()
                        .put("uid", "123")
                        .put("service_email", "service@argon.com"),
                new JsonObject().put("$set", new JsonObject().put("access_token", accessToken)
                        .put("refresh_token", refreshToken)), result -> {
                    if (!result.succeeded()) {
                        throw new RuntimeException("failed to update oauth token");
                    }
                });
    }

    @Override
    public void onError(BoxAPIConnection api, BoxAPIException error) {
        System.out.println("Failed to refresh box token " + error);
    }
}*/

package com.jinloes.data_streamer;

import com.box.sdk.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import org.apache.commons.lang3.ArrayUtils;
import org.bson.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jinloes on 9/15/15.
 */
public class BoxFileReader extends AbstractVerticle {
    private static final List<String> BOX_USER_FIELDS = new ArrayList<>();

    static {
        BOX_USER_FIELDS.add("role");
        BOX_USER_FIELDS.add("tracking_codes");
        BOX_USER_FIELDS.add("can_see_managed_users");
        BOX_USER_FIELDS.add("is_sync_enabled");
        BOX_USER_FIELDS.add("is_exempt_from_device_limits");
        BOX_USER_FIELDS.add("is_exempt_from_login_verification");
        BOX_USER_FIELDS.add("enterprise");
        BOX_USER_FIELDS.add("type");
        BOX_USER_FIELDS.add("name");
        BOX_USER_FIELDS.add("login");
        BOX_USER_FIELDS.add("created_at");
        BOX_USER_FIELDS.add("modified_at");
        BOX_USER_FIELDS.add("language");
        BOX_USER_FIELDS.add("space_amount");
        BOX_USER_FIELDS.add("space_used");
        BOX_USER_FIELDS.add("max_upload_size");
        BOX_USER_FIELDS.add("status");
        BOX_USER_FIELDS.add("job_title");
        BOX_USER_FIELDS.add("phone");
        BOX_USER_FIELDS.add("address");
        BOX_USER_FIELDS.add("avatar_url");
    }

    private final MongoClient mongoClient;
    private final OAuthMongoDao oAuthMongoDao;
    private static final String clientId = "hy20ez74722il7ib18uqj004bxo1p97i";
    private static final String clientSecret = "JAQhGIA80UxrEspjWPyMtysAW34FKtoD";
    private String accessToken;
    private String refreshToken;

    public BoxFileReader(MongoClient mongoClient, OAuthMongoDao oAuthMongoDao) {
        this.mongoClient = mongoClient;
        this.oAuthMongoDao = oAuthMongoDao;
    }

    @Override
    public void start() {
        vertx.eventBus().consumer("readBox", this::readBox);
        org.bson.Document oauth = oAuthMongoDao.getOAuthInfo();
        accessToken = oauth.getString("access_token");
        refreshToken = oauth.getString("refresh_token");
    }

    public void readBox(Message<JsonObject> message) {
        BoxAPIConnection api = new BoxAPIConnection(clientId, clientSecret, accessToken, refreshToken);
        api.addListener(new BoxAPIConnectionListener() {
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
        });
        BoxUser adminUser = BoxUser.getCurrentUser(api);
        for (BoxUser.Info userInfo : BoxUser.getAllEnterpriseUsers(api, null,
                BOX_USER_FIELDS.toArray(ArrayUtils.EMPTY_STRING_ARRAY))) {
            System.out.println();
            System.out.println(userInfo.getLogin());
            System.out.println(userInfo.getID());
            if (!adminUser.getID().equals(userInfo.getID())) {
                api.setRequestInterceptor(
                        request -> {
                            request.addHeader("As-User", userInfo.getID());
                            return null;
                        }
                );
            } else {
                api.setRequestInterceptor(null);
            }
            BoxFolder userRoot = BoxFolder.getRootFolder(api);
            for (BoxItem.Info child : userRoot.getChildren()) {
                System.out.println(child.getName());
            }
            System.out.println(userRoot.getID());
            System.out.println();
        }
        message.reply("");
    }
}

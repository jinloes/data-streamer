package com.jinloes.data_streamer;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * Created by jinloes on 9/16/15.
 */
public class OAuthMongoDao {
    private final MongoDatabase db;

    public OAuthMongoDao() {
        MongoClient mongoClient = new MongoClient();
        db = mongoClient.getDatabase("DEFAULT_DB");
    }

    public void updateOAuthCredentials(String accessToken, String refreshToken, String csUid, String serviceEmail) {
        Bson criteria = Filters.and(Filters.eq("uid", csUid), Filters.eq("service_email", serviceEmail));
        Document document = new Document("$set", new Document("access_token", accessToken)
                .append("refresh_token", refreshToken));
        db.getCollection("oauth_tokens").updateOne(criteria, document);
    }

    public Document getOAuthInfo() {
        return db.getCollection("oauth_tokens").find(Filters.and(Filters.eq("service_email", "service@argon.com"),
                Filters.eq("uid", "123"))).iterator().next();
    }
}

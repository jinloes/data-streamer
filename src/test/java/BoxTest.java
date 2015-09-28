import com.box.sdk.*;
import org.junit.Test;

/**
 * Created by jinloes on 9/15/15.
 */
public class BoxTest {
    private static final String clientId = "hy20ez74722il7ib18uqj004bxo1p97i";
    private static final String clientSecret = "JAQhGIA80UxrEspjWPyMtysAW34FKtoD";
    private String accessToken = "UfUEmUh8SUBSbJRdAA9rGJN0RVBhWA7p";
    private String refreshToken = "cnrYhcyIZeDuZiw6ycBotddhjbHljd3c6FbGJyMzL19ay5AQCzYAz7o9u7jABCla";
    @Test
    public void boxTest() {
        try {
            BoxAPIConnection apiConnection = new BoxAPIConnection(clientId, clientSecret, accessToken, refreshToken);
            apiConnection.addListener(new BoxAPIConnectionListener() {
                @Override
                public void onRefresh(BoxAPIConnection api) {
                    accessToken = api.getAccessToken();
                    System.out.println("Access token refresh: " + api.getAccessToken());
                    refreshToken = api.getRefreshToken();
                    System.out.println("Refresh token refresh: " + api.getRefreshToken());
                }

                @Override
                public void onError(BoxAPIConnection api, BoxAPIException error) {

                }
            });
            BoxFolder root = BoxFolder.getRootFolder(apiConnection);
            for (BoxItem.Info child : root.getChildren()) {
                System.out.println(child.getName());
            }
            for (BoxUser.Info userInfo : BoxUser.getAllEnterpriseUsers(apiConnection)) {
                System.out.println();
                System.out.println(userInfo.getLogin());
                System.out.println(userInfo.getID());
                BoxAPIConnection userApiConnection = new BoxAPIConnection(clientId, clientSecret, accessToken, refreshToken);
                userApiConnection.setRequestInterceptor(new RequestInterceptor() {
                    @Override
                    public BoxAPIResponse onRequest(BoxAPIRequest request) {
                        request.addHeader("As-User", userInfo.getID());
                        return null;
                    }
                });
                BoxFolder userRoot = BoxFolder.getRootFolder(userApiConnection);
                for (BoxItem.Info child : userRoot.getChildren()) {
                    System.out.println(child.getName());
                }
                //System.out.println("Impersonated user's folder: " + boxUser.getInfo().getUploadEmail().getEmail());
                System.out.println(userRoot.getID());
                //System.out.println(boxUser.getInfo().getName());
                System.out.println();
            }
        } catch (Exception e){
            System.out.println(e);
        }
    }
}

package sn.common;

import org.apache.http.client.utils.URIBuilder;

import java.net.URISyntaxException;

/**
 * HTTP Service paramaters
 * Created by Sumanth on 17/10/14.
 */
public class HttpServiceParams {
    private final String protocol;
    private final String host;
    private final int port;
    private final String url;

    public HttpServiceParams(final String protocol,String host, int port, String url) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;
        this.url = url;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUrl() {
        return url;
    }

    public String buildUri(){
        try {

             String urlStr =
                     new URIBuilder().setHost(host).setPort(port).setPath(url).build().toString();

            return protocol+":"+urlStr;
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static void main(String [] args){
        HttpServiceParams serviceParams = new HttpServiceParams("http","localhost",8080,"/genie/V0/jobs");
        System.out.println(serviceParams.buildUri());

    }

}

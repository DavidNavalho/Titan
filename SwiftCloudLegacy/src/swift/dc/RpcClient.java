package swift.dc;

import static sys.net.api.Networking.Networking;
import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.utils.Threading;

public class RpcClient {

    public static void main(String[] args) {

        Sys.init();

        RpcEndpoint endpoint = Networking.rpcConnect().toDefaultService();
        final Endpoint server = Networking.resolve("localhost", DCConstants.SURROGATE_PORT);

        for (;;) {
            endpoint.send(server, new Request(), new Handler() {

                public void onFailure( RpcHandle handler ) {
                    System.out.println("Client Send failed...");
                }

                public void onReceive(RpcHandle conn, Reply r) {
                    System.out.println("Client Got: " + r + " from:" + conn.remoteEndpoint());
//                    conn.reply(new Reply());
                }

            });
            Threading.sleep(5000);
        }

    }
}

package sys.dht.api;

import sys.net.api.Endpoint;

/**
 * The public interface of the DHT.
 * 
 * @author smd (smd@fct.unl.pt)
 * 
 */
public interface DHT {

	Endpoint localEndpoint();

	/**
	 * Requests the DHT to resolve the endpoint of the DHT node responsible for the given key.
	 * @param key - the key to be resolved
	 * @param timeout - the maximum amount of time in milliseconds to block for the answer.
	 * @return the endpoint of the node rensponsible for the key, or null if no answer was received in the allowed timeout.
	 */
	Endpoint resolveKey(final Key key, int timeout);
	
	void send(final Key key, final Message msg);

	void send(final Key key, final Message msg, ReplyHandler handler);

	interface Key {

		long longHashValue();
	}

	interface Message {

		void deliverTo(final Handle conn, final Key key, final MessageHandler handler);

	}

	interface MessageHandler {

		void onFailure();

		void onReceive(final Handle conn, final Key key, final Message request);

	}

	interface Reply {

		void deliverTo(final Handle conn, final ReplyHandler handler);

	}

	interface ReplyHandler {

		void onFailure();

		void onReceive(final Reply msg);

		void onReceive(final Handle conn, final Reply reply);
	}

	interface Handle {

		/**
		 * Tells if this handle awaits a reply.
		 * 
		 * @return true/false if the connection awaits a reply or not
		 */
		boolean expectingReply();

		/**
		 * Send a (final) reply message using this connection
		 * 
		 * @param msg
		 *            the reply being sent
		 * @return true/false if the reply was successful or failed
		 */
		boolean reply(final Reply msg);

		/**
		 * Send a reply message using this connection, with further message
		 * exchange round implied.
		 * 
		 * @param msg
		 *            the reply message
		 * @param handler
		 *            the handler that will be notified upon the arrival of an
		 *            reply (to this reply)
		 * @return true/false if the reply was successful or failed
		 */
		boolean reply(final Reply msg, final ReplyHandler handler);
	}

	abstract class AbstractReplyHandler implements ReplyHandler {

		@Override
		public void onFailure() {
		}

		@Override
		public void onReceive(Reply msg) {
		}

		@Override
		public void onReceive(Handle conn, Reply reply) {
		}
	}

	abstract class AbstractMessageHandler implements MessageHandler {

		@Override
		public void onFailure() {
		}

		@Override
		public void onReceive(Handle conn, Key key, Message request) {
		}
	}
}

package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 * Faster
	 * is not necessarily better, so don't sweat speed. Focus on safety.
	 */
	public static final int SLEEP = 1000;
	private ArrayList<Request> requests;
	private String keyspace;
	private String my_id;
	Cluster cluster;
	Session session;

	/**
	 * All Gigapaxos apps must either support a no-args constructor or a
	 * constructor taking a String[] as the only argument. Gigapaxos relies on
	 * adherence to this policy in order to be able to reflectively construct
	 * customer application instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect.
	 *             Optional args[1] and args[2]
	 * @throws IOException
	 */
	public MyDBReplicableAppGP(String[] args) throws IOException {
		// TODO: setup connection to the data store and keyspace
		this.my_id = args[0];
		this.keyspace = args[0];
		this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		this.session = cluster.connect(this.keyspace);
		this.requests = new ArrayList<Request>();
		//throw new RuntimeException("Not yet implemented");
	}

	/**
	 * Refer documentation of {@link Replicable#execute(Request, boolean)} to
	 * understand what the boolean flag means.
	 * <p>
	 * You can assume that all requests will be of type {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
	 *
	 * @param request
	 * @param b
	 * @return
	 */
	@Override
	public boolean execute(Request request, boolean b) {
		return this.execute(request);
	}

	/**
	 * Refer documentation of
	 * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
	 *
	 * @param request
	 * @return
	 */
	@Override
	public boolean execute(Request request) {
		try {
			this.requests.add(request);
			RequestPacket requestPacket = ((RequestPacket) request);
			String query = requestPacket.requestValue;
			ResultSet result_set = this.session.execute(query);

			return true;

		} catch (Exception e) {
			System.out.println("Got an error. Should not happen");
			return false;

		}

	}

	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
	public String checkpoint(String s) {
		ArrayList<String> resultList = new ArrayList<String>();
		try {
		// TODO:
		String select_all_data_query = "SELECT * FROM " + this.keyspace + ".grade";
		ResultSet results_set = this.session.execute(select_all_data_query);

		for (Row result: results_set) {
			String real_id = result.getString("id");
			String real_event = result.getString("events");
			String ind_query = "insert into grade (" + real_id + ", " + real_event + ");";
			resultList.add(ind_query);
		}

		for (Request ind_request : requests) {
			resultList.add(ind_request.toString());
		}

		//We need to clear the list now
		this.requests.clear();

		//Send the string so that it can be used in the restore method as s1
		String final_result = "";
		for (String ind_query_result: resultList) {
			final_result = final_result + ind_query_result;
		}
		return final_result;
		} catch (Exception e) {
		// We need to handle the case wherein the session.execute() fails. We then send only those 
		//requests that have been executed by this server.
		for (Request ind_request : requests) {
			resultList.add(ind_request.toString());
		}

		//We need to clear the list now
		this.requests.clear();

		//Send the string so that it can be used in the restore method as s1
		String final_result = "";
		for (String ind_query_result: resultList) {
			final_result = final_result + ind_query_result;
		}
		return final_result;
		}
		//throw new RuntimeException("Not yet implemented");
	}

	/**
	 * Refer documentation of {@link Replicable#restore(String, String)}
	 *
	 * @param s
	 * @param s1
	 * @return
	 */
	@Override
	public boolean restore(String s, String s1) {
		// TODO:
		try
		{
		ArrayList<String> strList = new ArrayList<String>(Arrays.asList(s1.split(";")));
		
		if (s1.equals(new String("{}"))) {
			return true;
		}

		for (String ind_query: strList) {
			String real_query = ind_query + ";";
			session.execute(real_query);
		}
		return true;
	}
	catch (Exception e)
	{
		System.out.println("Exception thrown in restore method ");
		return false;
	}

	}


	/**
	 * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
	 * .RequestPacket will be used by Grader, so you don't need to implement
	 * this method.}
	 *
	 * @param s
	 * @return
	 * @throws RequestParseException
	 */
	@Override
	public Request getRequest(String s) throws RequestParseException {
		return null;
	}

	/**
	 * @return Return all integer packet types used by this application. For an
	 * example of how to define your own IntegerPacketType enum, refer {@link
	 * edu.umass.cs.reconfiguration.examples.AppRequest}. This method does not
	 * need to be implemented because the assignment Grader will only use
	 * {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}
}

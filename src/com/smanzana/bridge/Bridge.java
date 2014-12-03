package com.smanzana.bridge;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.smanzana.Project3.Frame.Frame;
import com.smanzana.Project3.Node.Bridge.STDMessage;
import com.smanzana.Project3.Utils.CircularList;

/**
 * Java version of the bridge. This runs separately than the rings, and connects to bridge nodes within
 * the rings. It handles all the routing of frames.
 * <p><b>[Dependencies: Project3]</b></p>
 * @author Skyler
 *
 */
public class Bridge {
	
	private static class EmbeddedBridge {
		
		/**
		 * Keeps track of which socket is used to received data from the embedded bridge node
		 */
		private Socket inputSocket;
		
		/**
		 * The socket that we use to send data to the bridge.
		 */
		private Socket outputSocket;
		
		public EmbeddedBridge(Socket in, Socket out) {
			this.inputSocket = in;
			this.outputSocket = out;
		}
		
//		public boolean contains(Socket sock) {
//			SocketAddress addr = sock.getRemoteSocketAddress();
//			if (addr.equals(inputSocket.getRemoteSocketAddress()) || addr.equals(outputSocket.getRemoteSocketAddress())) {
//				return true;
//			}
//			return false;
//		}
		
		@Override
		public String toString() {
			return "Input: " + inputSocket.toString() + "\nOutput: " + outputSocket.toString() + "\n";
		}
	}
	
	/**
	 * Table that maps addresses to sockets (LANS)
	 */
	private Map<Byte, EmbeddedBridge> lookupTable;
	
	/**
	 * A list of all embedded embeddedBridges we're still currently receiving from.
	 * Bridges are removed from the list as they send in their FINISH frame
	 * Once the list is empty, we know we can send the remote kill.
	 */
	private CircularList<EmbeddedBridge> embeddedBridges;
	
	/**
	 * A list of all known connected bridges. This list does not get modified, and is used when
	 * doing extensive searching through the bridge's history.
	 */
	private List<EmbeddedBridge> knownConnections;
	
//	/**
//	 * Keeps track of all sockets used for output
//	 */
//	private List<Socket> outputSockets;
	
	/**
	 * Private copy of what port this bridge will be listening on
	 */
	private static int myPort;
	
	
	
	public static void main(String[] args) {
		if (args.length == 0) {
			usage();
			return;
		}
		System.out.print("Loading config...");
		myPort = parseConfig(args[0]);
		if (myPort == -1) {
			System.out.println("Unable to find config file: " + args[0]);
			return;
		}
		System.out.println(" done");	
		
		System.out.print("Bridge initializing...");		
		Bridge bridge = new Bridge();
		System.out.println(" done");	

		System.out.println("Creating connections:");
		int count = Integer.parseInt(args[1]);
		
		if (count <= 0) {
			System.out.println("Invalid number of rings: " + count);
			return;
		}

		Socket in, out;
		ServerSocket sSock;
		try {
			sSock = new ServerSocket();
			sSock.bind(new InetSocketAddress("127.0.0.1", myPort));
		} catch (IOException e) {
			System.out.println("Error encountered when creating and binding server socket!");
			return;
		}
		
		for (int i = 0; i < count; i++) {
			try {
				in = sSock.accept();
				System.out.println("Got a connection!");
				byte[] offset = new byte[1];
				in.getInputStream().read(offset);
				
				//we got out offset. The port is actually 7000 + offset. Connect a socket to that address and we'll have
				//enough to create an EmbeddedBridge
				out = new Socket();
				out.connect(new InetSocketAddress("127.0.0.1", 7000 + offset[0]));
				
				//we got in and out! Create our bridge!
				EmbeddedBridge br = new EmbeddedBridge(in, out);
				
				//register our new bridge
				bridge.embeddedBridges.add(br);
				bridge.knownConnections.add(br);
			} catch (IOException e) {
				
			}
		}
		
		try {
			sSock.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		
//		for (String arg : args) {
//			try {
//				port = Integer.parseInt(arg);
//			} catch (NumberFormatException e) {
//				e.printStackTrace();
//				System.out.println("Error when parsing port: " + arg);
//				return;
//			}
//
//			System.out.print("connecting on port " + port + "...     ");
//			if (!bridge.connect(port)) {
//				//error!
//				return;
//			}
//			System.out.println("done!");
//		}

		System.out.println("Bridge initialized!");
		bridge.start();
		
	}
	
	/**
	 * Prints out the proper command-line call used to create the bridge
	 */
	private static void usage() {
		System.out.println("Usage:");
		System.out.println("java -jar bridge.jar bridgeconf.conf numberOfConnections");
	}
	
	
	
	
	
	public Bridge() {
		
		lookupTable = new HashMap<Byte, EmbeddedBridge>();
		embeddedBridges = new CircularList<EmbeddedBridge>();
		knownConnections = new LinkedList<EmbeddedBridge>();
	}
	
	public void start() {
		boolean cont = true;
		while (cont) {
			try {
				cont = nextInput();
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Encountered an IO Exception when trying to fetch/process input!");
			}
			
//			try {
//				sleep(10);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} //just to give the thread a little break between having nothing.
		}
	}
	
	private static int parseConfig(String fileName) {
		File config = new File(fileName);
		if (!config.exists()) {
			return -1;
		}
		
		Scanner in;
		try {
			in = new Scanner(config);
		} catch (FileNotFoundException e) {
			System.out.println("Unnexpected error occured when opening a scanner over the config!");
			return -1;
		}
		
		int p = in.nextInt();
		in.close();
		return p;
	}
	
//	protected boolean connect(int port) {
//		Socket sock;
//		
//		try {
//			sock = new Socket("127.0.0.1", port);
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//			System.out.println("Encountered an Unknown Host Exception!");
//			return false;
//		} catch (IOException e) {
//			e.printStackTrace();
//			System.out.println("Error when trying to connect on port " + port);
//			return false;
//		}
//		
//		//everything went okay?
//		
//		//add socket to our list of sockets
//		if (sock != null) { //just incase
//			embeddedBridges.add(sock);
//			outputSockets.add(sock);
//		}
//		
//		return true;
//	}
	
	
	
	
	/**
	 * Fetches and processes input.
	 * <p>Care is given not to starve any input sockets (queues) using a {@link com.smanzana.Project3.Utils.CircularList CircularList}.
	 * </p>
	 * @throws IOException Error occurs when trying to fetch an input stream
	 */
	private boolean nextInput() throws IOException {
		if (embeddedBridges == null) {
			System.out.println("Invalid call to nextInput in bridge! List is null!");
			return true;
		}
		
		if (embeddedBridges.isEmpty()) {
			return true;
		}
		
		Socket sock = null;
		
		//Prepare yourself for some magic.
		//We use a for loop that goes up to <i>size</i> times. It doesn't care what the current index of
		//out list is. That's all handled in the CircularList class. Instead, we just call 'next' up to
		//<i>size</i> times looking for a socket with a frame ready to process. This will stop us from
		//looping infinitely until we get a frame!
		EmbeddedBridge bridge = null;
		for (int i = 0; i < embeddedBridges.size(); i++) {
			bridge = embeddedBridges.next();
			sock = bridge.inputSocket;
			int size = sock.getReceiveBufferSize();
			int avail = sock.getInputStream().available();
//			if (size-avail < 50) {
//				System.out.println("Buffer for socket: " + sock + " getting full (" + avail + " / " + size + ")");
//			}
			if (avail < Frame.headerLength) {
				//not ready to be looked at, so move on
				sock = null;
				continue;
			}
			
			//have input ready to be processed!
			break;
		}
		
		if (sock == null) {
			//went through the whole list once and didn't get any available input;
			return true;
		}
		
		byte[] frame = getFrame(sock); 
		
		if (bridge == null) {
			System.out.println("Encountered strange error: unable to match a socket with an embeddedBridge!");
		}
		return processFrame(bridge, frame);
	}
	
	/**
	 * Fetches a complete frame from the passed socket.<br />
	 * Frame is going to be complete, or null. No partial frames will be passed unless the header is corrupt.
	 * @param sock The socket to fetch the frame from.
	 * @return a byte array containing exactly the whole frame, or null on error
	 * @throws IOException Unable to get input stream
	 */
	private byte[] getFrame(Socket sock) throws IOException {
		if (sock == null) {
			System.out.println("Tried to get frame from a null socekt!!!");
			return null;
		}
		
		InputStream input = sock.getInputStream();
		
		if (input.available() < Frame.headerLength) {
			System.out.println("Trying to get a frame from an un-ready socket!"); //this is tested against above, but just to be sure
			return null;
		}
		
		byte[] header = receive(input, Frame.headerLength);
		byte[] body = receive(input, (Frame.Header.getSize(header) & 0xFF) + 1, 200 + (Frame.Header.getSize(header) * 20));
		
		byte[] frame = new byte[header.length + body.length];
		
		int i;
		for (i = 0; i < header.length; i++) {
			frame[i] = header[i];
		}
		for (; i < frame.length; i++) {
			frame[i] = body[i - header.length];
		}
		
		return frame;
	}
	
	
	private boolean processFrame(EmbeddedBridge returnBridge, byte[] frame) throws IOException {
		if (frame == null) {
			System.out.println("Tried to process a null frame in the bridge!");
			return false;
		}
		
		if (checkFrame(frame)) {
			//we have a bad frame!
			System.out.println("Bridge has detected a bad frame!");
			return false;
		}
		
		//First special check: is it a token?
		if (Frame.Header.isToken(Frame.getHeader(frame))) {
			//we got a token. Pass it back to the LAN it came from
			send(returnBridge, frame);
		}
		
		//Second special check: Did we (or a monitor) send it? Is it a command frame? Is it from a embedded-bridge?
		if (Frame.Header.getSource(Frame.getHeader(frame)) == 0) {
			//Check for a bridge frame from an embedded bridge
			byte[] data = Frame.getData(frame);
			if (data.length == 1) {
				/**
				 * frame from source 0 with size of 1. Assume it's an inter-bridge communication message
				 * (see {@link com.smanzana.Project3.Node.Bridge Bridge}
				 */
				STDMessage msg = com.smanzana.Project3.Node.Bridge.STDMessage.fromId(data[0]);
				System.out.println("Got a communication frame: " + msg.name());
				switch (msg) {
				case FINISH:
				default:
					embeddedBridges.remove(returnBridge); //remove that socket from the list of active rings, if it's there
					if (embeddedBridges.isEmpty()) {
						//close down the rings
						byte[] killFrame = assembleFrame(STDMessage.KILL);
						flood(killFrame);
						return false;
					}
					break;
				}
				return true;
			}
			
			
			//for now, just drain it. 
			//TODO figure out what the monitor is saying
			//TODO remove dead nodes from our lookup table
			//drain it by not retransmitting it
			return true;
		}
		
		//Done with the special checks. We know it isn't a control frame. Instead we just route it and make sure the
		//source is in our routing table
		updateRoutingTable(returnBridge, frame);
		
		byte FS = Frame.getFrameStatus(frame);
		if (FS != 0) {
			//NAK or ACK frame. Ignore cause we lie and produce ACKS
			return true;
		}
		
		
		Byte address = Frame.Header.getDestination(Frame.getHeader(frame));
		
		//!!!!!!!!!!!!
		//Set the monitor bit to 0, to avoid silly errors involving a message getting unlucky and passing two monitors
		//in two different networks before reaching its goal
		//!!!!!!!!!!!!
		byte AC = frame[0];
		if ((AC & 8) != 0) {
			//something there. Subtract 8 if positive. I hate how everything is signed
			if (AC > 0) {
				AC = (byte) (AC - 8);
			} else {
				AC = (byte) (AC + 8);
			}
		}
		//AC = (byte) (AC & 247);//XXXX XXXX & 1111 0111 = XXXX 1XXXX  -- set the monitor bit to 0 and leave everything else the same
		frame[0] = AC;
		
		EmbeddedBridge output = lookupTable.get(address);
		
		//last check: are we going to move this frame across LANs? If so:
		//is it an ack? We send out fake ones, so we drain those
		//do we need to send a fake ack?
		if (output == null || returnBridge != output) {
			if (Frame.getFrameStatus(frame) != 0) {
				//FS other than 0 means this is an ack (or NAK) frame coming back. Drain it.
				return true;
				//TODO what if it gets rejected?!!?!?!?!
			}
			//This is the original frame, so generate a fake ACK for the sender
			ack(returnBridge, frame);
		}
		
		if (output == null) {
			//not in lookup table!
			//fluuuuuuudddddddddddddd
			//we don't need to do anything special, as the ack that the node sends back will be used to update
			//our routing table
			flood(frame);
		}
		else {
			send(output, frame);
		}
		
		return true;
		
	}
	
	/**
	 * Tries to get <i>size</i> bytes from the passed input stream.<br />
	 * This implementation has no timeout, and will hang forever until it gets the required number of bytes!
	 * @param input The input stream to receive from
	 * @param size how many bytes are to be read
	 * @return a newly-created array list containing the next <i>size</i> bytes from the stream
	 * @throws IOException Error occurs while interacting with the input stream.
	 */
	private byte[] receive(InputStream input, int size) throws IOException {
		
		byte[] data = new byte[size];
		
		while (input.available() < size) {
			try {
				wait(10); //to avoid trying every single cycle, sleep a few milliseconds and then try again
			} catch (InterruptedException e) {
				//Do nothing. An interupted thread is an interupted thread. WE are sleeping to waste time so this doesn't matter.
				//at least how I understand it.
			}
		}
		
		try {
			input.read(data);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error when trying to read data from socket! ERR50");
			return null;
		}
		
		
		return data;
	}
	
	/**
	 * Tries to get <i>size</i> bytes from the passed input stream.<br />
	 * At most, this method will wait <i>timeout</i> milliseconds before timing out. If a timeout occurs,
	 * null is returned.<br />
	 * <p>It is worth mentioning that the timeout is handled in 10-millisecond steps. If the passed timeout is not a multiple
	 * of 10 milliseconds, it will be effectively rounded up to the next multiple of 10. The timeout is handled this way for
	 * efficiency in assesing the input stream status and to ease the strain caused by this thread. The thread sleeps inbetween
	 * every 10 millisecond period.</p>
	 * @param input The input stream to receive from
	 * @param size how many bytes are to be read
	 * @param timeout how many miliseconds to wait before issuing a time out
	 * @return a newly-created array list containing the next <i>size</i> bytes from the stream, or null on timeout
	 * @throws IOException Error occurs while interacting with the input stream.
	 */
	private byte[] receive(InputStream input, int size, int timeout) throws IOException {
		byte[] data = new byte[size];
		int sleepCount = 0;
		
		while (input.available() < size) {
			try {
				wait(10); //to avoid trying every single cycle, sleep a few milliseconds and then try again
				//We have a timeout to keep in mind. We have to keep how long we've been sleeping and check it each time
				sleepCount++;
				if (sleepCount >= (timeout) / 10) {
					break; //if we've slept past our timeout, break;
				}
			} catch (InterruptedException e) {
				//Do nothing. An interupted thread is an interupted thread. WE are sleeping to waste time so this doesn't matter.
				//at least how I understand it.
			}
		}
		
		//check for timeout
		if (input.available() < size) {
			System.out.println("Timed out when bridge tried to receive a frame!");
			return null;
		}
		
		try {
			input.read(data);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error when trying to read data from socket! ERR50");
			return null;
		}
		
		
		return data;
	}
	
	/**
	 * Sends the passed frame over the passed socket.
	 * @param output What socket to send the frame through
	 * @param frame What frame to send through the socket :D
	 * @throws IOException Error when trying to use the output stream of the socket
	 */
	private void send(EmbeddedBridge output, byte[] frame) throws IOException {
		if (output == null || frame == null) {
			System.out.println("Tried to send null frame on null socket!");
			return;
		}
		
		OutputStream out = output.outputSocket.getOutputStream();
		
		System.out.print("Sending... ");
		out.write(frame);
		out.flush();
		System.out.println("done");
	}
	
	/**
	 * Floods the passed frame to all registered output sockets.<br />
	 * Output sockets are determined by creating a {@link java.util.Set Set} of the values of the nodeTable
	 * @param frame The complete frame to flood to all LANS
	 * @throws IOException Exception caused when fetching output streams of the sockets
	 */
	private void flood(byte[] frame) throws IOException {
		if (knownConnections.isEmpty()) {
			System.out.println("Tried to flood a message, but nobody exists to flood to!");
		}
		for (EmbeddedBridge bridge : knownConnections) {
			send(bridge, frame);
		}
	}
	
	/**
	 * Checks to make sure the frame is in a valid format
	 * @param frame
	 * @return
	 */
	private boolean checkFrame(byte[] frame) {
		return false;
		/*
		 * I don't really know how to detect a bad frame. I had this problem in Project 2 :(
		 * Since we're using byte data with no delimiters, it's hard to tell if the header or body is screwed up.
		 * Even if something essential like the SIZE portion of the header is messed up, we wouldn't really know. We would
		 * just MAYBE end up timing out when trying to get the body. Otherwise, we'd accept too little bytes or take part of
		 * the next frame header as our body. :( Right?
		 */
	}
	
	/**
	 * Checks to make sure that the routing table currently associates the source-address of the frame with the
	 * socket it came from.<br />
	 * This currently only checks against the address key already existing in the map. If the frame came from a socket that
	 * is different than its registered socket, nothing will change.
	 * @param sock
	 * @param frame
	 */
	private void updateRoutingTable(EmbeddedBridge bridge, byte[] frame) {
		if (bridge == null || frame == null) {
			return;
		}
		
		Byte address = Frame.Header.getSource(Frame.getHeader(frame));

		//check if we already have the address registered
		if (lookupTable.containsKey(address)) {
			//TODO what if the address is coming from a new socket??
			return;
		}
		
		//address not registered!
		lookupTable.put(address,  bridge);
	}
	
	/**
	 * Sends an acknowledgment frame back to the source of the passed frame through the passed socket.
	 * @param sock The socket that the frame came through originally
	 * @param frame The frame
	 * @throws IOException If error occurs when trying to send the frame (see {@link #send(Socket, byte[]) send()})
	 */
	private void ack(EmbeddedBridge bridge, byte[] frame) throws IOException {
		//an acknowledgment frame is the same frame with the FS byte changed to 2 -- accepted.
		byte[] ackFrame = frame.clone(); //if we don't clone, we'll change the frame!!!
		ackFrame[ackFrame.length - 1] = 2;
		send(bridge, ackFrame);
	}
	
	private byte[] assembleFrame(STDMessage msg) {
		byte[] frame = new byte[7];
		//Because this frame is immediately picked up and sorted out by the embedded bridges,
		//we don't need to worry much about the monitor bits, the token bit/byte, etc.
		//We only need to worry about:
		frame[0] = 0;
		frame[1] = 0;
		frame[2] = 0;
		frame[3] = 0; //the source, which we set to 0 as standard
		frame[4] = 1; //the size
		frame[5] = msg.id; //our message
		frame[6] = 0;
		
		return frame;
	}
	
//	/**
//	 * Tries to get an EmbeddedBridge that contains to the passed socket.<br />
//	 * This method checks both the input and output sockets, so the passed socket can be either and will
//	 * still return the EmbeddedBridge associated with it.<br />
//	 * If multiple EmbeddedBridges exist with the same registered sockets, this will only return the first.
//	 * @param sock
//	 * @return
//	 */
//	private EmbeddedBridge getEmbeddedBridge(Socket sock) {
//		EmbeddedBridge bridge = null;
//		for (EmbeddedBridge br : knownConnections) {
//			br = embeddedBridges.next();
//			if (br.contains(sock)) {
//				bridge = br;
//				break;
//			}
//		}
//		return bridge;
//	}
}

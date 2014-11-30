package com.smanzana.bridge;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;

import com.smanzana.Project3.Frame.Frame;
import com.smanzana.Project3.Utils.CircularList;

/**
 * Java version of the bridge. This runs separately than the rings, and connects to bridge nodes within
 * the rings. It handles all the routing of frames.
 * <p><b>[Dependencies: Project3]</b></p>
 * @author Skyler
 *
 */
public class Bridge {
	
	/**
	 * Table that maps addresses to sockets (LANS)
	 */
	private Map<Byte, Socket> lookupTable;
	
	/**
	 * A list of all registered sockets
	 */
	private CircularList<Socket> socketList;
	
	
	
	public static void main(String[] args) {
		System.out.println("Bridge initializing...");
		if (args.length == 0) {
			usage();
			return;
		}
		
		int port;
		for (String arg : args) {
			try {
				port = Integer.parseInt(arg);
			} catch (NumberFormatException e) {
				e.printStackTrace();
				System.out.println("Error when parsing port: " + arg);
				return;
			}
			
			connect(port);
			
		}
		
	}
	
	/**
	 * Prints out the proper command-line call used to create the bridge
	 */
	private static void usage() {
		System.out.println("Usage:");
		System.out.println("java -jar bridge.jar port1 [port2] [port3] [...]");
	}
	
	private static void connect(int port) {
		try {
			Socket sock = new Socket("127.0.0.1", port);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return;
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error when trying to connect on port " + port);
			return;
		}
		
		//everything went okay?
		
		//add socket to our list of sockets
		
		
	}
	
	/**
	 * Fetches and processes input.
	 * <p>Care is given not to starve any input sockets (queues) using a {@link com.smanzana.Project3.Utils.CircularList CircularList}.
	 * </p>
	 * @throws IOException Error occurs when trying to fetch an input stream
	 */
	private void nextInput() throws IOException {
		if (SocketList == null) {
			System.out.println("Invalid call to nextInput in bridge! List is null!");
			return;
		}
		
		if (SocketList.isEmpty()) {
			return;
		}
		
		Socket sock = null;
		
		//Prepare yourself for some magic.
		//We use a for loop that goes up to <i>size</i> times. It doesn't care what the current index of
		//out list is. That's all handled in the CircularList class. Instead, we just call 'next' up to
		//<i>size</i> times looking for a socket with a frame ready to process. This will stop us from
		//looping infinitely until we get a frame!
		for (int i = 0; i < SocketList.size(); i++) {
			sock = SocketList.next();
			if (sock.getInputStream().available() < Frame.headerLength) {
				//not ready to be looked at, so move on
				sock = null;
				continue;
			}
			
			//have input ready to be processed!
			break;
		}
		
		if (sock == null) {
			//went through the whole list once and didn't get any available input;
			return;
		}
		
		byte[] frame = getFrame(sock);
		processFrame(sock, frame);
		
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
		byte[] body = receive(input, Frame.Header.getSize(header) + 1, 200 + (Frame.Header.getSize(header) * 20));
		
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
	
	
	private void processFrame(Socket returnSocket, byte[] frame) throws IOException {
		if (frame == null) {
			System.out.println("Tried to process a null frame in the bridge!");
			return;
		}
		
		if (checkFrame(frame)) {
			//we have a bad frame!
			System.out.println("Bridge has detected a bad frame!");
			return;
		}
		
		//First special check: is it a token?
		if (Frame.Header.isToken(Frame.getHeader(frame))) {
			//we got a token. Pass it back to the LAN it came from
			send(returnSocket, frame);
		}
		
		//Second special check: Did we (or a monitor) send it? Is it a command frame? Is it routing table info?
		if (Frame.Header.getSource(Frame.getHeader(frame)) == 0) {
			//for now, just drain it. 
			//TODO figure out what the monitor is saying
			//TODO remove dead nodes from our lookup table
			//drain it by not retransmitting it
			return;
		}
		
		//Done with the special checks. We know it isn't a control frame. Instead we just route it and make sure the
		//source is in our routing table
		updateRoutingTable(returnSocket, frame);
		
		
		Byte address = Frame.Header.getDestination(Frame.getHeader(frame));
		Socket output = lookupTable.get(address);
		
		//last check: are we going to move this frame across LANs? If so:
		//is it an ack? We send out fake ones, so we drain those
		//do we need to send a fake ack?
		if (output == null || returnSocket != output) {
			if (Frame.getFrameStatus(frame) != 0) {
				//FS other than 0 means this is an ack (or NAK) frame coming back. Drain it.
				return;
				//TODO what if it gets rejected?!!?!?!?!
			}
			//This is the original frame, so generate a fake ACK for the sender
			ack(returnSocket, frame);
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
	private void send(Socket output, byte[] frame) throws IOException {
		if (output == null || frame == null) {
			System.out.println("Tried to send null frame on null socket!");
			return;
		}
		
		OutputStream out = output.getOutputStream();
		
		out.write(frame);
		out.flush();
	}
	
	/**
	 * Floods the passed frame to all registered output sockets.<br />
	 * Output sockets are determined by creating a {@link java.util.Set Set} of the values of the nodeTable
	 * @param frame The complete frame to flood to all LANS
	 * @throws IOException Exception caused when fetching output streams of the sockets
	 */
	private void flood(byte[] frame) throws IOException {
		for (int i = 0; i < SocketList.size(); i++) {
			send(SocketList.next(), frame);
		}
		//go through socket list once complete time so we don't mess up our index. Send frame on each socket.
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
	private void updateRoutingTable(Socket sock, byte[] frame) {
		if (sock == null || frame == null) {
			return;
		}
		
		Byte address = Frame.Header.getSource(Frame.getHeader(frame));

		//check if we already have the address registered
		if (lookupTable.containsKey(address)) {
			//TODO what if the address is coming from a new socket??
			return;
		}
		
		//address not registered!
		lookupTable.put(address,  sock);
	}
	
	/**
	 * Sends an acknowledgment frame back to the source of the passed frame through the passed socket.
	 * @param sock The socket that the frame came through originally
	 * @param frame The frame
	 * @throws IOException If error occurs when trying to send the frame (see {@link #send(Socket, byte[]) send()})
	 */
	private void ack(Socket sock, byte[] frame) throws IOException {
		//an acknowledgment frame is the same frame with the FS byte changed to 2 -- accepted.
		byte[] ackFrame = frame.clone(); //if we don't clone, we'll change the frame!!!
		ackFrame[ackFrame.length - 1] = 2;
		send(sock, ackFrame);
	}
}

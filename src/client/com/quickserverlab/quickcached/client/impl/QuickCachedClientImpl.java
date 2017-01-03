package com.quickserverlab.quickcached.client.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.quickserverlab.quickcached.client.CASResponse;
import com.quickserverlab.quickcached.client.CASValue;
import com.quickserverlab.quickcached.client.GenericResponse;
import com.quickserverlab.quickcached.client.MemcachedClient;
import com.quickserverlab.quickcached.client.MemcachedException;
import com.quickserverlab.quickcached.client.TimeoutException;
import org.quickserver.net.client.BlockingClient;
import org.quickserver.net.client.ClientInfo;
import org.quickserver.net.client.Host;
import org.quickserver.net.client.HostList;
import org.quickserver.net.client.SocketBasedHost;
import org.quickserver.net.client.loaddistribution.LoadDistributor;
import org.quickserver.net.client.loaddistribution.impl.HashedLoadPattern;
import org.quickserver.net.client.monitoring.HostMonitor;
import org.quickserver.net.client.monitoring.HostStateListener;
import org.quickserver.net.client.monitoring.impl.SocketMonitor;
import org.quickserver.net.client.pool.BlockingClientPool;
import org.quickserver.net.client.pool.PoolableBlockingClient;
import org.quickserver.net.client.pool.PooledBlockingClient;

/**
 *
 * @author akshath
 */
public class QuickCachedClientImpl extends MemcachedClient {
	private static final Logger logger = Logger.getLogger(QuickCachedClientImpl.class.getName());
	
    protected static final int FLAGS_GENRIC_STRING = 0;
	protected static final int FLAGS_GENRIC_OBJECT = 1;
        
	protected static String charset = "ISO-8859-1";//"utf-8";
	private String hostList;
	private boolean binaryConnection = false;
	private int poolSize = 5;
	private int minPoolSize = 4;
	private int idlePoolSize = 8;
	private int maxPoolSize = 16;
	
	private long noOpTimeIntervalMiliSec = 1000*60;//60 sec
	private int hostMonitoringIntervalInSec = 15;//15sec
	private int maxIntervalForBorrowInSec = 4;//4 sec
	private int logPoolIntervalTimeMin = 10;//10min
	private BlockingClientPool blockingClientPool;
	private HostList hostListObj;
	private boolean debug;

	private void updatePoolSizes() {
		minPoolSize = poolSize / 2;
		idlePoolSize = poolSize;
		maxPoolSize = poolSize * 2;
	}

	public void setUseBinaryConnection(boolean flag) {
		binaryConnection = flag;
	}

	public void setConnectionPoolSize(int size) {
		poolSize = size;
		updatePoolSizes();
	}

	public void setAddresses(String list) {
		hostList = list;
	}

	public void addServer(String list) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void removeServer(String list) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void init() throws IOException {
		hostListObj = new HostList("memcached_" + hostList);
		updatePoolSizes();
                
		String servers[] = hostList.split(" ");
		String server[] = null;
		SocketBasedHost sbh = null;
		for (int i = 0; i < servers.length; i++) {
                        if(servers[i].contains(":")==false) {
                            continue;
                        }
                    
			server = servers[i].split(":");
                        
                        if(server[0].trim().isEmpty()) {
                            continue;
                        }
                        if(server[1].trim().isEmpty()) {
                            continue;
                        }
                        
			try {
				sbh = new SocketBasedHost(server[0].trim(), 
					Integer.parseInt(server[1].trim()));
			} catch (Exception ex) {
				Logger.getLogger(QuickCachedClientImpl.class.getName()).log(
					Level.SEVERE, "Error: " + ex, ex);
			}
			sbh.setTimeout((int)getDefaultTimeoutMiliSec());
			sbh.setRequestText("version\r\n");
			sbh.setResponseTextToExpect("VERSION ");
			
			hostListObj.add(sbh);
		}
		
		final SocketMonitor sm = new SocketMonitor();
		
		final LoadDistributor ld = new LoadDistributor(hostListObj);
		ld.setLoadPattern(new HashedLoadPattern());
		
		PoolableBlockingClient poolableBlockingClient = new PoolableBlockingClient() {
			public HostMonitor getHostMonitor() {
				return sm;
			}

			public LoadDistributor getLoadDistributor() {
				return ld;
			}

			public BlockingClient createBlockingClient(SocketBasedHost host) {
				BlockingClient bc = new BlockingClient();
				try {
					bc.connect(host.getInetAddress().getHostAddress(), host
							.getInetSocketAddress().getPort());
					bc.getSocket().setTcpNoDelay(true);
					bc.getSocket().setSoTimeout((int) getDefaultTimeoutMiliSec());
					bc.getSocket().setSoLinger(true, 10);
					return bc;
				} catch (Exception ex) {
					logger.log(Level.WARNING, "Error: " + ex, ex);
					return null;
				}
			}

			public boolean closeBlockingClient(BlockingClient blockingClient) {
				if (blockingClient == null) {
					return false;
				}
				try {
					blockingClient.close();
					return true;
				} catch (IOException ex) {
					logger.log(Level.WARNING, "Error: " + ex, ex);
				}
				return false;
			}

			public boolean sendNoOp(BlockingClient blockingClient) {
				if (blockingClient == null) {
					return false;
				}

				try {
					blockingClient.sendBytes("version\r\n", charset);
					String recData = blockingClient.readCRLFLine();
					if (recData == null) {
						return false;
					}

					if (recData.startsWith("VERSION ")) {
						return true;
					} else {
						return false;
					}
				} catch (IOException e) {
					logger.log(Level.WARNING, "Error: " + e, e);
					return false;
				}
			}

			public long getNoOpTimeIntervalMiliSec() {
				return noOpTimeIntervalMiliSec;
			}

			public int getHostMonitoringIntervalInSec() {
				return hostMonitoringIntervalInSec;
			}

			public boolean isBlockWhenEmpty() {
				return false;
			}

			public int getMaxIntervalForBorrowInSec() {
				return maxIntervalForBorrowInSec;
			}
		};
		
		blockingClientPool = new BlockingClientPool("memcached_"+hostList,
				poolableBlockingClient);
		blockingClientPool.setDebug(isDebug());

		blockingClientPool.setMinPoolSize(minPoolSize);
		blockingClientPool.setIdlePoolSize(idlePoolSize);
		blockingClientPool.setMaxPoolSize(maxPoolSize);

		HostStateListener hsl = new HostStateListener() {
			public void stateChanged(Host host, char oldstatus, char newstatus) {
				if (oldstatus != Host.UNKNOWN) {
					logger.log(Level.SEVERE, "State changed: {0}; old state: {1};new state: {2}", 
						new Object[]{host, oldstatus, newstatus});
				} else {
					logger.log(Level.INFO, "State changed: {0}; old state: {1};new state: {2}", 
						new Object[]{host, oldstatus, newstatus});
				}
			}
		};
		blockingClientPool.getHostMonitoringService().addHostStateListner(hsl);
		blockingClientPool.setLogPoolStatsTimeInMinute(logPoolIntervalTimeMin);
		blockingClientPool.init();
	}

	public void stop() throws IOException {
		blockingClientPool.close();
		blockingClientPool = null;
	}

	private String sendDataOut(String key, String data) throws TimeoutException {
		ClientInfo ci = new ClientInfo();
		ci.setClientKey(key);

		PooledBlockingClient pbc = null;

		try {
			pbc = blockingClientPool.getBlockingClient(ci);
			if (pbc == null) {
				throw new TimeoutException("sdo: we do not have any client[pbc] to connect to server!");
			}

			BlockingClient bc = pbc.getBlockingClient();
			if (bc == null) {
				throw new TimeoutException("we do not have any client[bc] to connect to server!");
			}

			bc.sendBytes(data, charset);

			return bc.readCRLFLine();
		} catch (IOException e) {            
            if (pbc != null) {
				logger.log(Level.WARNING, "We had an ioerror will close client! " + e, e);
				pbc.close();
			}
            if(e instanceof TimeoutException) {
                throw (TimeoutException) e;
            } else {
                throw new TimeoutException("We had ioerror " + e);
            }	
		} finally {
			if (pbc != null) {
				blockingClientPool.returnBlockingClient(pbc);
			}
		}
	}

	private CASValue readDataOutCAS(String key, String data) throws TimeoutException {
		long casUnique = 0;

		ClientInfo ci = new ClientInfo();
		ci.setClientKey(key);

		PooledBlockingClient pbc = null;

		try {
			pbc = blockingClientPool.getBlockingClient(ci);
			if (pbc == null) {
				throw new TimeoutException("rdo: we do not have any client[pbc] to connect to server!");
			}

			BlockingClient bc = pbc.getBlockingClient();
			if (bc == null) {
				throw new TimeoutException("we do not have any client[bc] to connect to server!");
			}

			bc.sendBytes(data, charset);
			String resMain = bc.readCRLFLine();

			if (resMain == null) {
				throw new TimeoutException("we got null reply!");
			}

			/*
			 VALUE <key> <flags> <bytes> [<cas unique>]\r\n
			 <data block>\r\n
			 END\r\n
			 */

			if (resMain.startsWith("VALUE ")) {
				String cmdData[] = resMain.split(" ");
				if (cmdData.length < 4) {
					return null;
				}
				int flag = Integer.parseInt(cmdData[2]);
				int bytes = Integer.parseInt(cmdData[3]);


				if (cmdData.length >= 5) {
					casUnique = Long.parseLong(cmdData[4]);
				}

				byte[] dataBuff = bc.readBytes(bytes);

				//read the footer 7 char extra \r\nEND\r\n
				bc.readBytes(7);

				if (dataBuff == null) {
					throw new TimeoutException("we don't have data!");
				}

				if (flag == FLAGS_GENRIC_STRING) {
					return new CASValue(casUnique, new String(dataBuff, charset));
				} else {
					return new CASValue(casUnique, retriveObject(dataBuff));
				}
			} else if (resMain.equals("END")) {
				return null;
			} else {
				logger.log(Level.WARNING, "unknown res got! : {0}", resMain);
				throw new TimeoutException("unknown res got! : " + resMain);
			}
		} catch (IOException e) {
			if (pbc != null) {
				logger.log(Level.WARNING, "We had an ioerror will close client! " + e, e);
				pbc.close();
			}
            if(e instanceof TimeoutException) {
                throw (TimeoutException) e;
            } else {
                throw new TimeoutException("We had ioerror " + e);
            }			
		} finally {
			if (pbc != null) {
				blockingClientPool.returnBlockingClient(pbc);
			}
		}
	}

	private Object readDataOut(String key, String data) throws TimeoutException {
		CASValue v = readDataOutCAS(key, data);
		if (v != null) {
			return v.getValue();
		} else {
			return null;
		}
	}

	public void set(String key, int ttlSec, Object value, long timeoutMiliSec) 
			throws TimeoutException, MemcachedException {
		try {
			StringBuilder sb = new StringBuilder();

			byte valueBytes[] = null;
			int flag = -1;

			if (value instanceof String) {
				String arg = (String) value;
				valueBytes = arg.getBytes(charset);
				flag = FLAGS_GENRIC_STRING;
			} else {
				valueBytes = getObjectBytes(value);
				flag = FLAGS_GENRIC_OBJECT;
			}
			
			validateKey(key);
			validateValue(key, valueBytes);

			//<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
			sb.append("set ").append(key).append(" ").append(flag);
			sb.append(" ").append(ttlSec).append(" ").append(valueBytes.length);
			sb.append("\r\n");
			sb.append(new String(valueBytes, charset));
			sb.append("\r\n");

			String res = sendDataOut(key, sb.toString());
			if (res == null) {
				throw new TimeoutException("we got a null reply!");
			}

			if (res.equals("STORED") == false) {
				throw new MemcachedException(key + " was not stored[" + res + "]");
			}
		} catch (TimeoutException ex) {
			throw ex;
		} catch (MemcachedException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new TimeoutException("We had error " + ex);
		}
	}

	public boolean add(String key, int ttlSec, Object value, long timeoutMiliSec)
		throws TimeoutException {
		try {
			StringBuilder sb = new StringBuilder();

			byte valueBytes[] = null;
			int flag = -1;

			if (value instanceof String) {
				String arg = (String) value;
				valueBytes = arg.getBytes(charset);
				flag = FLAGS_GENRIC_STRING;
			} else {
				valueBytes = getObjectBytes(value);
				flag = FLAGS_GENRIC_OBJECT;
			}
			
			validateKey(key);
			validateValue(key, valueBytes);

			//<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
			sb.append("add ").append(key).append(" ").append(flag);
			sb.append(" ").append(ttlSec).append(" ").append(valueBytes.length);
			sb.append("\r\n");
			sb.append(new String(valueBytes, charset));
			sb.append("\r\n");

			String res = sendDataOut(key, sb.toString());
			if (res == null) {
				throw new TimeoutException("we got a null reply!");
			}

			if (res.equals("STORED") == false) {
				return false;
			} else {
				return true;
			}
		} catch (TimeoutException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new TimeoutException("We had error " + ex);
		}
	}

	public boolean replace(String key, int ttlSec, Object value, long timeoutMiliSec)
		throws TimeoutException {
		try {
			StringBuilder sb = new StringBuilder();

			byte valueBytes[] = null;
			int flag = -1;

			if (value instanceof String) {
				String arg = (String) value;
				valueBytes = arg.getBytes(charset);
				flag = FLAGS_GENRIC_STRING;
			} else {
				valueBytes = getObjectBytes(value);
				flag = FLAGS_GENRIC_OBJECT;
			}
			
			validateKey(key);
			validateValue(key, valueBytes);

			//<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
			sb.append("replace ").append(key).append(" ").append(flag);
			sb.append(" ").append(ttlSec).append(" ").append(valueBytes.length);
			sb.append("\r\n");
			sb.append(new String(valueBytes, charset));
			sb.append("\r\n");

			String res = sendDataOut(key, sb.toString());
			if (res == null) {
				throw new TimeoutException("we got a null reply!");
			}

			if (res.equals("STORED") == false) {
				return false;
			} else {
				return true;
			}
		} catch (TimeoutException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new TimeoutException("We had error " + ex);
		}
	}

	@Override
	public boolean append(String key, Object value, long timeoutMiliSec) throws TimeoutException {
		try {
			StringBuilder sb = new StringBuilder();

			byte valueBytes[] = null;
			int flag = -1;

			if (value instanceof String) {
				String arg = (String) value;
				valueBytes = arg.getBytes(charset);
				flag = FLAGS_GENRIC_STRING;
			} else {
				valueBytes = getObjectBytes(value);
				flag = FLAGS_GENRIC_OBJECT;
			}
			
			validateKey(key);
			validateValue(key, valueBytes);

			//<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
			sb.append("append ").append(key).append(" ").append(flag);
			sb.append(" ").append("0").append(" ").append(valueBytes.length);
			sb.append("\r\n");
			sb.append(new String(valueBytes, charset));
			sb.append("\r\n");

			String res = sendDataOut(key, sb.toString());
			if (res == null) {
				throw new TimeoutException("we got a null reply!");
			}

			if (res.equals("STORED") == false) {
				return false;
			} else {
				return true;
			}
		} catch (TimeoutException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new TimeoutException("We had error " + ex);
		}
	}

	public boolean append(long cas, String key, Object value, long timeoutMiliSec)
		throws TimeoutException {
		return append(key, value, timeoutMiliSec);
	}

	@Override
	public boolean prepend(String key, Object value, long timeoutMiliSec) throws TimeoutException {
		try {
			StringBuilder sb = new StringBuilder();

			byte valueBytes[] = null;
			int flag = -1;

			if (value instanceof String) {
				String arg = (String) value;
				valueBytes = arg.getBytes(charset);
				flag = FLAGS_GENRIC_STRING;
			} else {
				valueBytes = getObjectBytes(value);
				flag = FLAGS_GENRIC_OBJECT;
			}
			
			validateKey(key);
			validateValue(key, valueBytes);

			//<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
			sb.append("prepend ").append(key).append(" ").append(flag);
			sb.append(" ").append("0").append(" ").append(valueBytes.length);
			sb.append("\r\n");
			sb.append(new String(valueBytes, charset));
			sb.append("\r\n");

			String res = sendDataOut(key, sb.toString());
			if (res == null) {
				throw new TimeoutException("we got a null reply!");
			}

			if (res.equals("STORED") == false) {
				return false;
			} else {
				return true;
			}
		} catch (TimeoutException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new TimeoutException("We had error " + ex);
		}
	}

	public boolean prepend(long cas, String key, Object value, long timeoutMiliSec)
		throws TimeoutException {
		return prepend(key, value, timeoutMiliSec);
	}

	public Object get(String key, long timeoutMiliSec)
			throws MemcachedException, TimeoutException {
		
		validateKey(key);
			
		CASValue casv = gets(key, timeoutMiliSec);
		if (casv == null) {
			return null;
		} else {
			return casv.getValue();
		}
	}

	private String sendCmdOut(String key, String data) throws TimeoutException {
		ClientInfo ci = new ClientInfo();
		ci.setClientKey(key);

		PooledBlockingClient pbc = null;

		try {
			pbc = blockingClientPool.getBlockingClient(ci);
			if (pbc == null) {
				throw new TimeoutException("cmo: we do not have any client[pbc] to connect to server!");
			}

			BlockingClient bc = pbc.getBlockingClient();
			if (bc == null) {
				throw new TimeoutException("we do not have any client[bc] to connect to server!");
			}

			bc.sendBytes(data, charset);

			String resMain = bc.readCRLFLine();
			if (resMain == null) {
				throw new TimeoutException("we got null reply!");
			}

			return resMain;
		} catch (IOException e) {
			if (pbc != null) {
				logger.log(Level.WARNING, "We had an ioerror will close client! " + e, e);
				pbc.close();
			}
            if(e instanceof TimeoutException) {
                throw (TimeoutException) e;
            } else {
                throw new TimeoutException("We had ioerror " + e);
            }			
		} finally {
			if (pbc != null) {
				blockingClientPool.returnBlockingClient(pbc);
			}
		}
	}

	public boolean delete(String key, long timeoutMiliSec) throws TimeoutException {
		try {
			validateKey(key);
			
			StringBuilder sb = new StringBuilder();
			//delete <key> [noreply]\r\n
			sb.append("delete ").append(key).append("\r\n");

			String res = sendCmdOut(key, sb.toString());
			if (res == null) {
				throw new TimeoutException("we got a null reply!");
			}

			if (res.equals("DELETED")) {
				return true;
			} else {
				return false;
			}
		} catch (TimeoutException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new TimeoutException("We had error " + ex);
		}
	}

	private void sendCmdOutToAll(String data) throws TimeoutException {
		PooledBlockingClient pbc[] = blockingClientPool.getOneBlockingClientForAllActiveHosts();
		if (pbc == null) {
			throw new TimeoutException("we do not have any client array [pbc] to connect to server!");
		}

		for (int i = 0; i < pbc.length; i++) {
			try {
				BlockingClient bc = pbc[i].getBlockingClient();
				if (bc == null) {
					throw new TimeoutException("we do not have any client[bc] to connect to server!");
				}

				bc.sendBytes(data, charset);
			} catch (IOException e) {
				if (pbc != null) {
					logger.log(Level.WARNING, "We had an ioerror will close client! " + e, e);
					pbc[i].close();
				}
			} finally {
				blockingClientPool.returnBlockingClient(pbc[i]);
				pbc[i] = null;
			}
		}

	}

	public void flushAll() throws TimeoutException {
		try {
			StringBuilder sb = new StringBuilder();
			//noreply [noreply]\r\n
			sb.append("flush_all noreply").append("\r\n");

			sendCmdOutToAll(sb.toString());

		} catch (TimeoutException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new TimeoutException("We had error " + ex);
		}
	}

	public Object getBaseClient() {
		return null;
	}

	public Map getStats() throws Exception {
		Map<InetSocketAddress, Map<String, String>> map = new HashMap<InetSocketAddress, Map<String, String>>();

		PooledBlockingClient pbc[] = blockingClientPool.getOneBlockingClientForAllActiveHosts();
		if (pbc == null) {
			throw new TimeoutException("we do not have any client array [pbc] to connect to server!");
		}

		Map<String, String> inmap = null;
		for (int i = 0; i < pbc.length; i++) {
			try {
				BlockingClient bc = pbc[i].getBlockingClient();
				if (bc == null) {
					throw new TimeoutException("we do not have any client[bc] to connect to server!");
				}

				inmap = getStats(bc);

				map.put(pbc[i].getSocketBasedHost().getInetSocketAddress(), inmap);
			} catch (IOException e) {
				if (pbc != null) {
					logger.log(Level.WARNING, "We had an ioerror will close client! " + e, e);
					pbc[i].close();
				}
			} finally {
				blockingClientPool.returnBlockingClient(pbc[i]);
				pbc[i] = null;
			}
		}

		return map;
	}

	private Map<String, String> getStats(BlockingClient bc) throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		try {
			StringBuilder sb = new StringBuilder();
			//stats\r\n
			sb.append("stats").append("\r\n");

			bc.sendBytes(sb.toString(), charset);

			String line = null;
			String stats[] = null;
			while (true) {
				line = bc.readCRLFLine();
				if (line == null || line.equals("END")) {
					break;
				}
				//STAT <name> <value>\r\n
				if (line.startsWith("STAT ") == false) {
					throw new Exception("We had bad stats output!" + line);
				}
				stats = line.split(" ");
				map.put(stats[1], stats[2]);
			}

			return map;
		} catch (TimeoutException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new TimeoutException("We had error " + ex);
		}
	}

	public Map getVersions() throws TimeoutException {
		Map<InetSocketAddress, String> map = new HashMap<InetSocketAddress, String>();

		PooledBlockingClient pbc[] = blockingClientPool.getOneBlockingClientForAllActiveHosts();
		if (pbc == null) {
			throw new TimeoutException("we do not have any client array [pbc] to connect to server!");
		}

		String version = null;
		for (int i = 0; i < pbc.length; i++) {
			try {
				BlockingClient bc = pbc[i].getBlockingClient();
				if (bc == null) {
					throw new TimeoutException("we do not have any client[bc] to connect to server!");
				}

				version = getVersion(bc);

				map.put(pbc[i].getSocketBasedHost().getInetSocketAddress(), version);
			} catch (IOException e) {
				if (pbc != null) {
					logger.log(Level.WARNING, "We had an ioerror will close client! " + e, e);
					pbc[i].close();
				}
			} finally {
				blockingClientPool.returnBlockingClient(pbc[i]);
				pbc[i] = null;
			}
		}

		return map;
	}

	private String getVersion(BlockingClient bc) throws IOException, TimeoutException {
		Map<String, String> map = new HashMap<String, String>();
		try {
			StringBuilder sb = new StringBuilder();
			//version\r\n
			sb.append("version").append("\r\n");

			bc.sendBytes(sb.toString(), charset);

			String line = bc.readCRLFLine();
			if (line == null) {
				throw new TimeoutException("We had EOF");
			}
			//VERSION <version>\r\n

			return line.substring(8);
		} catch (TimeoutException ex) {
			throw ex;
		} catch (IOException ex) {
			throw ex;
		}
	}

	/**
	 * @return the debug
	 */
	public boolean isDebug() {
		return debug;
	}

	/**
	 * @param debug the debug to set
	 */
	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	protected static byte[] getObjectBytes(Object object) {
		ObjectOutputStream out = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			out = new ObjectOutputStream(bos);
			out.writeObject(object);
			return bos.toByteArray();
		} catch (Exception e) {
			logger.log(Level.WARNING, "Error: " + e, e);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException ex) {
					logger.log(Level.WARNING, "Error: " + ex, ex);
				}
			}
		}
		return null;
	}

	protected static Object retriveObject(byte bytes[]) {
		ObjectInputStream in = null;
		try {
			Object object;
			in = new ObjectInputStream(new ByteArrayInputStream(bytes));
			object = in.readObject();
			return object;
		} catch (Exception e) {
			logger.log(Level.WARNING, "Error: " + e, e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException ex) {
					logger.log(Level.WARNING, "Error: " + ex, ex);
				}
			}
		}
		return null;
	}

	@Override
	public boolean touch(String key, int ttlSec, long timeoutMiliSec) throws TimeoutException {
		try {
			StringBuilder sb = new StringBuilder();
			//touch <key> <exptime> [noreply]\r\n
			sb.append("touch ").append(key).append(" ").append(ttlSec).append("\r\n");

			ClientInfo ci = new ClientInfo();
			ci.setClientKey(key);

			String res = sendCmdOut(key, sb.toString());
			if (res == null) {
				throw new TimeoutException("we got a null reply!");
			}

			if (res.equals("TOUCHED")) {
				return true;
			} else {
				return false;
			}
		} catch (TimeoutException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new TimeoutException("We had error " + ex);
		}
	}

	@Override
	public Object gat(String key, int ttlSec, long timeoutMiliSec) throws TimeoutException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public CASValue gets(String key, long timeoutMiliSec)
			throws TimeoutException, MemcachedException {
		validateKey(key);
		
		CASValue casObject = null;
		try {
			StringBuilder sb = new StringBuilder();
			//get <key>*\r\n
			sb.append("get ").append(key).append("\r\n");

			casObject = readDataOutCAS(key, sb.toString());
		} catch (TimeoutException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new TimeoutException("We had error " + ex);
		}
		return casObject;
	}

	@Override
	public CASResponse cas(String key, Object value, int ttlSec, long cas, 
			long timeoutMiliSec) throws TimeoutException, MemcachedException {
		validateKey(key);
		
		try {
			StringBuilder sb = new StringBuilder();

			byte valueBytes[] = null;
			int flag = -1;

			if (value instanceof String) {
				String arg = (String) value;
				valueBytes = arg.getBytes(charset);
				flag = FLAGS_GENRIC_STRING;
			} else {
				valueBytes = getObjectBytes(value);
				flag = FLAGS_GENRIC_OBJECT;
			}

			//cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
			sb.append("cas ").append(key).append(" ").append(flag);
			sb.append(" ").append(ttlSec).append(" ").append(valueBytes.length);
			sb.append(" ").append(cas).append("\r\n");
			sb.append(new String(valueBytes, charset));
			sb.append("\r\n");

			String res = sendCmdOut(key, sb.toString());
			if (res == null) {
				throw new TimeoutException("we got a null reply!");
			}
			if (res.equals("STORED")) {
				return CASResponse.OK;
			} else if (res.equals("NOT_STORED")) {
				return CASResponse.NOT_STORED;
			} else if (res.equals("EXISTS")) {
				return CASResponse.EXISTS;
			} else if (res.equals("NOT_FOUND")) {
				return CASResponse.NOT_FOUND;
			} else {
				logger.warning("Unknown res: " + res);
				return CASResponse.ERROR;
			}
		} catch (TimeoutException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new MemcachedException("We had error " + ex);
		}
	}

	public Map<Integer, StringBuilder> getCommandsForKeys(Collection<String> keys)
		throws MemcachedException {
		List activeList = hostListObj.getActiveList();
		String commandString = "get ";
		if (activeList == null || activeList.isEmpty()) {
			throw new MemcachedException("No active list available to service requests");
		}
		int size = activeList.size();

		//Keys seperated depending on the host assigned for it
		//Assuming there will be no changes in the activeList size.
		Map<Integer, StringBuilder> commandsMap = new HashMap<Integer, StringBuilder>(size);
		try {
			for (String key : keys) {
				if (key == null) {
					//skip
				} else {
					//code from org.quickserver.net.client.loaddistribution.impl.HashedLoadPattern
					int hash = key.hashCode();
					int mod = hash % size;
					if (mod < 0) {
						mod = mod * -1;
					}
					//--

					if (commandsMap.get(mod) != null) {
						commandsMap.get(mod).append(" ").append(key);
					} else {
						commandsMap.put(mod, new StringBuilder(commandString).append(key));
					}
				}
			}
		} catch (Exception e) {
			//log exception
			throw new MemcachedException("Invalid key/s found " + e);
		}
		return commandsMap;
	}

	@Override
	public <T> Map<String, T> getBulk(Collection<String> keyCollection,
			long timeoutMiliSec) throws TimeoutException, MemcachedException {
		return get(keyCollection, timeoutMiliSec);
	}

	public <T> Map<String, T> get(Collection<String> keyCollection,
			long timeoutMiliSec) throws TimeoutException, MemcachedException {
		if (keyCollection == null || keyCollection.isEmpty()) {
			return null;
		}

		Map<Integer, StringBuilder> commands = getCommandsForKeys(keyCollection);
		if (commands == null) {
			throw new MemcachedException("Error: bad key passed!");
		}

		//TODO look to re-use the pool
		ExecutorService exec = null;
		Map<String, T> result = new HashMap<String, T>();
		try {
			exec = Executors.newFixedThreadPool(commands.size());
			List<Future<List<GenericResponse>>> res = new ArrayList<Future<List<GenericResponse>>>();			

			for (StringBuilder commandSB : commands.values()) {
				String command = commandSB.toString();
				String commandKeys[] = command.split(" ");
				if (commandKeys.length >= 2) {				
					StringBuilder cmd = new StringBuilder(command).append("\r\n");

					ClientInfo ci = new ClientInfo();
					ci.setClientKey(commandKeys[(commandKeys.length - 1)]);//last key
					PooledBlockingClient pbc = null;
					try {
						pbc = blockingClientPool.getBlockingClient(ci);
						if (pbc == null) {
							throw new TimeoutException(
								"we do not have any client[pbc] to connect to server!");
						}

						BlockingClient bc = pbc.getBlockingClient();
						if (bc == null) {
							throw new TimeoutException(
								"we do not have any client[bc] to connect to server!");
						}

						res.add(exec.submit(
							new MultiLineResCommandRunner(blockingClientPool, cmd.toString(), pbc)));

					} catch (Exception e) {
						if (pbc != null) {
							logger.log(Level.WARNING, "We had an ioerror will close client! " + e, e);
							pbc.close();
						}
						throw new TimeoutException("We had ioerror " + e);
					}
				} else {
					logger.log(Level.WARNING, "Invalid commands passed {0}", command);
				}
			}

			try {
				for (Future<List<GenericResponse>> futureObj : res) {
					List<GenericResponse> resultPart = futureObj.get(
						timeoutMiliSec, TimeUnit.MILLISECONDS);
					for (GenericResponse entry : resultPart) {
						if (entry != null) {
							result.put(entry.getKey(), (T) entry.getValue());
						}
					}
				}
			} catch (java.util.concurrent.TimeoutException ex) {
				throw new TimeoutException("Timeout Exception: " + ex);
			} catch (InterruptedException ex) {
				throw new MemcachedException("Connection Interrupted: " + ex);
			} catch (ExecutionException ex) {
				throw new MemcachedException("Error occured " + ex);
			}
		} finally {
			if(exec!=null) {
				exec.shutdown();
			}
		}
		return result;
	}

	/* incr */
	@Override
	public long increment(String key, int delta, long timeoutMiliSec) 
			throws TimeoutException, MemcachedException {
		return increment(key, delta, delta, 0, timeoutMiliSec);
	}

	@Override
	public long increment(String key, int delta, long defaultValue, 
			long timeoutMiliSec) throws TimeoutException, MemcachedException {
		return increment(key, delta, defaultValue, 0, timeoutMiliSec);
	}

	@Override
	public long increment(String key, int delta, long defaultValue, 
			int ttlSec, long timeoutMiliSec) throws TimeoutException, 
			MemcachedException {
		validateKey(key);
		
		long result;
		try {
			StringBuilder sb = new StringBuilder();
			//incr <key> <value> [noreply]\r\n
			sb.append("incr ").append(key).append(" ").append(delta).append("\r\n");

			String res = sendCmdOut(key, sb.toString());
			if (res == null) {
				throw new MemcachedException("we got a null reply!");
			}

			if (res.equals("NOT_FOUND")) {
				boolean flag = add(key, ttlSec, ""+defaultValue, timeoutMiliSec);
				if(flag==false) {
					throw new MemcachedException("add failed "+key);
				}
				res = ""+defaultValue;
			}

			try {
				result = Long.parseLong(res);
			} catch (Exception e) {
				throw new MemcachedException("invalid response: "+res);
			}
		} catch (TimeoutException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new TimeoutException("We had error " + ex);
		}
		return result;
	}

	@Override
	public void incrementWithNoReply(String key, int delta) 
			throws MemcachedException {
		validateKey(key);
		
		try {
			increment(key, delta, delta, 0, getDefaultTimeoutMiliSec());
		} catch (TimeoutException ex) {
			logger.log(Level.WARNING, "Error: "+ ex);
		}
	}

	/* decr */
	@Override
	public long decrement(String key, int delta, long defaultValue, 
			long timeoutMiliSec) throws TimeoutException, MemcachedException {
		return decrement(key, delta, defaultValue, 0, timeoutMiliSec);
	}

	@Override
	public long decrement(String key, int delta, long defaultValue, 
			int ttlSec, long timeoutMiliSec) throws TimeoutException, MemcachedException {
		validateKey(key);
		
		long result = -1;
		try {
			StringBuilder sb = new StringBuilder();
			//decr <key> <value> [noreply]\r\n
			sb.append("decr ").append(key).append(" ").append(delta).append("\r\n");

			ClientInfo ci = new ClientInfo();
			ci.setClientKey(key);

			String res = sendCmdOut(key, sb.toString());
			if (res == null) {
				throw new TimeoutException("we got a null reply!");
			}

			if (res.equals("NOT_FOUND")) {
				add(key, ttlSec, ""+defaultValue, timeoutMiliSec);
				return defaultValue;
			}

			try {
				result = Long.parseLong(res);
			} catch (Exception e) {
				throw new MemcachedException("invalid response "+res);
			}
		} catch (TimeoutException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new TimeoutException("We had error " + ex);
		}
		return result;
	}

	@Override
	public long decrement(String key, int delta, long timeoutMiliSec) 
			throws TimeoutException, MemcachedException {
		return decrement(key, delta, 0, 0, timeoutMiliSec);
	}

	@Override
	public void decrementWithNoReply(String key, int delta) 
			throws MemcachedException {
		try {
			decrement(key, delta, 0, 0, getDefaultTimeoutMiliSec());
		} catch (TimeoutException ex) {
			logger.log(Level.WARNING, "Error: "+ex);
		}
	}
}

class MultiLineResCommandRunner implements Callable<List<GenericResponse>> {
	private static final Logger logger = Logger.getLogger(MultiLineResCommandRunner.class.getName());
	private String command;
	private PooledBlockingClient pbc;
	private BlockingClientPool blockingClientPool;

	public MultiLineResCommandRunner(BlockingClientPool blockingClientPool, 
				String command, PooledBlockingClient pbc) {
		this.command = command;
		this.blockingClientPool = blockingClientPool;
		this.pbc = pbc;
	}

	public List<GenericResponse> call() throws Exception {
		boolean exceptionOccured = true;
		List<GenericResponse> resultList = new ArrayList<GenericResponse>();
		try {
			if (pbc == null) {
				throw new TimeoutException("we do not have any client[pbc] to connect to server!");
			}

			BlockingClient bc = pbc.getBlockingClient();
			if (bc == null) {
				throw new TimeoutException("we do not have any client[bc] to connect to server!");
			}

			logger.log(Level.FINE, "Cmd rcvd : {0}", command);
			bc.sendBytes(command, QuickCachedClientImpl.charset);

			/*
			 VALUE <key> <flags> <bytes> [<cas unique>]\r\n
			 <data block>\r\n
			 VALUE <key> <flags> <bytes> [<cas unique>]\r\n
			 <data block>\r\n
			 VALUE <key> <flags> <bytes> [<cas unique>]\r\n
			 <data block>\r\n
			 END\r\n
			 */

			while (true) {
				String resMain = bc.readCRLFLine();
				if (resMain == null) {
					throw new MemcachedException("we got null reply!");
				}
				//System.out.println("resMain "+ resMain);
				if (resMain.startsWith("VALUE ")) {
					String cmdData[] = resMain.split(" ");
					if (cmdData.length < 4) {
						throw new MemcachedException("Bad res : "+resMain);
					}
					String valueKey = cmdData[1];
					int flag = Integer.parseInt(cmdData[2]);
					int bytes = Integer.parseInt(cmdData[3]);
					long casValue = 0;
					if(cmdData.length>=5) {
						casValue = Long.parseLong(cmdData[4]);
					}

					byte[] dataBuff = null;
					
					if(bytes>0) {
						dataBuff = bc.readBytes(bytes);
						if (dataBuff == null) {
							throw new TimeoutException("we don't have data!");
						}						
					}
					
					//skip \r\n
					bc.readBytes(2);
					
					if(bytes==0) {
						continue;
					}

					if (flag == QuickCachedClientImpl.FLAGS_GENRIC_STRING) {
						resultList.add(new GenericResponse(valueKey, casValue,
							new String(dataBuff, QuickCachedClientImpl.charset)));						
					} else if (flag == QuickCachedClientImpl.FLAGS_GENRIC_OBJECT) {
						resultList.add(new GenericResponse(valueKey, casValue,
							QuickCachedClientImpl.retriveObject(dataBuff)));						
					} else {
						throw new MemcachedException("Bad flag! "+flag);
					}
				} else if (resMain.equals("END")) {
					break;
				} else {
					logger.log(Level.WARNING, "unknown res got! : {0}", resMain);
					throw new MemcachedException("unknown res got! : " + resMain);
				}
			}

			exceptionOccured = false;
			return resultList;
		} catch (IOException e) {
			logger.log(Level.WARNING, "IOError: " + e, e);
			if (pbc != null) {
				exceptionOccured = true;
			}
			throw new TimeoutException("We had ioerror " + e);
		} catch (Exception e) {
			logger.log(Level.WARNING, "Error: " + e, e);
			if (pbc != null) {
				exceptionOccured = true;
			}
			throw new TimeoutException("We had error " + e);
		} finally {
			if (exceptionOccured) {
				if (pbc != null) {
					logger.log(Level.WARNING, "We had an error will close client! ");
					pbc.close();
				}
			}
			if (pbc != null) {
				blockingClientPool.returnBlockingClient(pbc);
			}
		}
	}
}
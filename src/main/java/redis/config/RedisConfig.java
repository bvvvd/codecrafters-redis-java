package redis.config;

import redis.RedisException;

public class RedisConfig {
    private static final String REPLICATION_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private String dir = Constants.DEFAULT_DIR;
    private String dbFileName = Constants.DEFAULT_RDB_FILENAME;
    private int port = Constants.DEFAULT_PORT;
    private String role = "master";
    private String masterHost;
    private int masterPort;

    @Override
    public String toString() {
        return "RedisConfig{" +
               "dir='" + dir + '\'' +
               ", dbFileName='" + dbFileName + '\'' +
               '}';
    }

    public RedisConfig(String[] args) {
        if (args != null && args.length > 0) {
            for (int i = 0; i < args.length; i += 2) {
                if (args[i].equalsIgnoreCase("--dir")) {
                    if (i + 1 < args.length) {
                        dir = args[i + 1];
                    } else {
                        throw new RedisException("Missing value for 'dir' argument");
                    }
                }

                if (args[i].equalsIgnoreCase("--dbfilename")) {
                    if (i + 1 < args.length) {
                        dbFileName = args[i + 1];
                    } else {
                        throw new RedisException("Missing value for 'dbfilename' argument");
                    }
                }

                if (args[i].equalsIgnoreCase("--port")) {
                    if (i + 1 < args.length) {
                        try {
                            int portValue = Integer.parseInt(args[i + 1]);
                            if (portValue < 0 || portValue > 65535) {
                                throw new RedisException("Port number must be between 0 and 65535");
                            }
                            this.port = portValue;
                        } catch (NumberFormatException e) {
                            throw new RedisException("Invalid port number: " + args[i + 1]);
                        }
                    } else {
                        throw new RedisException("Missing value for 'port' argument");
                    }
                }

                if (args[i].equalsIgnoreCase("--replicaOf")) {
                    if (i + 1 < args.length) {
                        role = "slave";
                        String masterInfo = args[i + 1];
                        String[] split = masterInfo.split(" ");
                        masterHost = split[0];
                        try {
                            int portValue = Integer.parseInt(split[1]);
                            if (portValue < 0 || portValue > 65535) {
                                throw new RedisException("Port number must be between 0 and 65535");
                            }
                            this.masterPort = portValue;
                        } catch (NumberFormatException e) {
                            throw new RedisException("Invalid port number: " + split[1]);
                        }
                    } else {
                        throw new RedisException("Missing value for 'replicaOf' argument");
                    }
                }
            }
        }
    }

    public String getDir() {
        return dir;
    }

    public String getDbFileName() {
        return dbFileName;
    }

    public int getPort() {
        return port;
    }

    public String getRole() {
        return role;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public String getReplicationId() {
        return REPLICATION_ID;
    }
}

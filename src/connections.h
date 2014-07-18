#ifdef __cplusplus
extern "C" {
#endif

#define CONN_DEFAULT_TIMEOUT	0		// Leave at system default
#define CONN_QUICK_TIMEOUT	2000		// For connections on localhost or LAN

int open_socket(const char *host, int port);
int open_connection(const char *host, int port, unsigned int timeout);
int open_lsocket(const char *filename);
int open_fifo(const char *filename);

int write_socket(int fd, char *buf, int len);
int read_socket(int fd, char *buf, int len, int ignore_timeout);

#ifdef __cplusplus
}
#endif

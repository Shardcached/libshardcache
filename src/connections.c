/*
 * \file
 *
 * \brief Connection setup
 */

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <syslog.h>
#include <unistd.h>

#include <sys/param.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <log.h>

#include "connections.h"

#ifndef INADDR_LOOPBACK
#define INADDR_LOOPBACK 0x7f000001
#endif

#ifndef INADDR_NONE
#define INADDR_NONE 0xffffffff
#endif


/**
 * \brief Convert "localhost" or "localhost:smtp" and store it in sockaddr.
 * \returns 0 if sockaddr is valid or -1 otherwise.
 */
int
string2sockaddr(const char *host, int port, struct sockaddr_in *sockaddr)
{
    u_int32_t ip = htonl(INADDR_LOOPBACK);
    errno = EINVAL;

    if (host) {
	char host2[512];
	char *p;
	char *pe;

	strncpy(host2, host, sizeof(host2)-1);
	p = strchr(host2, ':');

	if (p) {				// check for <host>:<port>
	    *p = '\0';				// point to port part
	    p++;
	    port = strtol(p, &pe, 10);		// convert string to number
	    if (*pe != '\0') {			// did not match complete string? try as string
		struct servent *e = getservbyname(p, "tcp");
		if (!e) {
		    errno = ENOENT;		// to avoid errno == 0 in error case
		    return -1;
		}
		port = ntohs(e->s_port);
	    }
	}

        if (strcmp(host2, "*") == 0) {
            ip = INADDR_ANY;
        } else {
            if (!inet_aton(host2, (struct in_addr *)&ip)) {
                struct hostent *e = gethostbyname(host2);
		if (!e || e->h_addrtype != AF_INET) {
		    errno = ENOENT;		// to avoid errno == 0 in error case
		    return -1;
		}
                ip = ((unsigned long *) (e->h_addr_list[0]))[0];
            }
        }
    }
    if (port == 0)
	return -1;
    else
	port = htons(port);

    bzero(sockaddr, sizeof(struct sockaddr_in));
#ifndef __linux
    sockaddr->sin_len = sizeof(struct sockaddr_in);
#endif
    sockaddr->sin_family = AF_INET;
    sockaddr->sin_addr.s_addr = ip;
    sockaddr->sin_port = port;

    return 0;
}
/*!
 * \brief Open a listen socket.
 * \param host hostname to listen on
 * \param port port to listen on
 * \returns file handle for socket to call accept() on or -1 otherwise (errno is set).
 *
 * \note Examples of valid port combinations: ("*", 3456), ("localhost", 3456),
 * or ("10.0.0.9", 4546).
 */
int
open_socket(const char *host, int port)
{
    int val = 1;
    struct sockaddr_in sockaddr;
    int sock;

    errno = EINVAL;
    if (host == NULL || strlen(host) == 0 || port == 0)
	return -1;

    sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == -1)
	return -1;

    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &val,  sizeof(val));

    if (string2sockaddr(host, port, &sockaddr) == -1
	|| bind(sock, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) == -1) {
	shutdown(sock, SHUT_RDWR);
	close(sock);
	return -1;
    }

    listen(sock, -1);
    fcntl(sock, F_SETFD, FD_CLOEXEC);

    return sock;
}

/*!
 * \brief Writes to a socket
 * \param fd socket
 * \param buf buffer to write
 * \param len size of the buffer
 * \returns number of written bytes (or 0) on success, -1 on error (errno is se to the underlying error)
 */
int write_socket(int fd, char *buf, int len) {
    int wb = 0;
    int ofx = 0;
    do {
        len -= wb;
        ofx += wb;
        wb =  write(fd, buf+ofx, len);
        if (wb == -1) {
            if (errno != EINTR || errno != EAGAIN) {
                NOTICE("write on fd %d failed: %s", fd, strerror(errno));
                return -1;
            }
            wb = 0;
        } else if (wb == 0) {
            break;
        }
    } while (wb != len);
    return wb;
}

/*!
 * \brief Read from a socket
 * \param fd socket
 * \param buf buffer where to store the read data
 * \param len pointer to an integer indicating the size of the buffer in input
 *            and the actual size written on output
 * \returns the number of read bytes (or 0) on success, -1 on error (errno is se to the underlying error)
 */
int read_socket(int fd, char *buf, int len) {
    int rb = 0;
    rb =  read(fd, buf, len);
    if (rb == -1 && (errno != EINTR || errno != EAGAIN)) { 
        NOTICE("Read on fd %d failed: %s", fd, strerror(errno));
        return -1;
    }
    return rb >= 0 ? rb : 0;
}


/*!
 * \brief Open a TCP connection to a client.
 * \param host hostname
 * \param port port number
 * \param timeout timeout for connection (send and receive)
 * \returns file handle on success, or -1 otherwise (errno is set).
 *
 * \note Examples for valid host and port combinations: ("test.com", 1099),
 * ("test.com:1099", 0), or ("10.0.0.10", 1099).
 */
int
open_connection(const char *host, int port, unsigned int timeout)
{
    int val = 1;
    struct sockaddr_in sockaddr;
    int sock;

    errno = EINVAL;
    if (host == NULL || strlen(host) == 0 || port == 0)
	return -1;

    sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == -1)
	return -1;

    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &val,  sizeof(val));
    if (timeout > 0) {
	struct timeval tv = { timeout, 0 };
	if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == -1
	    || setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == -1)
	    INFO("%s:%d: Failed to set timeout to %d\n", host, port, timeout);
    }

    if (string2sockaddr(host, port, &sockaddr) == -1
	|| connect(sock, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) == -1) {
	shutdown(sock, SHUT_RDWR);
	close(sock);
	return -1;
    }

    fcntl(sock, F_SETFD, FD_CLOEXEC);

    return sock;
}

/*!
 * \brief Open a UNIX domain socket.
 * \param filename filename for socket
 * \returns file handle for socket to call accept() on or -1 otherwise (errno is set).
 */
int
open_lsocket(const char *filename)
{
    struct sockaddr_un sockaddr;
    int sock;

    errno = EINVAL;
    if (filename == NULL || strlen(filename) == 0)
	return -1;

    sock = socket(PF_UNIX, SOCK_STREAM, 0);
    if (sock == -1)
	return -1;

    unlink(filename);

    sockaddr.sun_family = AF_UNIX;
    strncpy(sockaddr.sun_path, filename, sizeof(sockaddr.sun_path));

    if (bind(sock, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) == -1
	|| listen(sock, -1) == -1) {
	shutdown(sock, SHUT_RDWR);
	close(sock);
	return -1;
    }

    fcntl(sock, F_SETFD, FD_CLOEXEC);

    return sock;
}

/*!
 * \brief Open a FIFO.
 * \param filename FIFO file name
 * \returns file handle on success, or -1 otherwise (errno is set).
 */

int
open_fifo(const char *filename)
{
    struct stat sb;
    int fd;

    if (mkfifo(filename, S_IFIFO | 0600) != 0) {
	if (errno == EEXIST) {
	    if (stat(filename, &sb) == -1) {
		errno = EEXIST;		// reset errno to the previous value
		return -1;
	    } else if (!S_ISFIFO(sb.st_mode)) {
		return -1;
	    }
	} else {
	    return -1;
	}
    }

    fd = open(filename, O_RDWR|O_EXCL|O_NONBLOCK);
    if (fd == -1)
	return -1;

    fcntl(fd, F_SETFD, FD_CLOEXEC);

    return fd;
}

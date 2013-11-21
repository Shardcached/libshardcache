#ifndef __IOMUX_H__
#define __IOMUX_H__

#ifdef __cplusplus
extern "C" {
#endif

#define IOMUX_DEFAULT_TIMEOUT 1

extern int iomux_hangup;

typedef struct __iomux iomux_t;
typedef void (*iomux_cb_t)(iomux_t *iomux, void *priv);

//! \brief iomux callbacks structure
typedef struct __iomux_callbacks {
    void (*mux_input)(iomux_t *iomux, int fd, void *data, int len, void *priv);
    void (*mux_output)(iomux_t *iomux, int fd, void *priv);
    void (*mux_timeout)(iomux_t *iomux, int fd, void *priv);
    void (*mux_eof)(iomux_t *iomux, int fd, void *priv);
    void (*mux_connection)(iomux_t *iomux, int fd, void *priv);
    void *priv;
} iomux_callbacks_t;

iomux_t *iomux_create(void);
int  iomux_add(iomux_t *iomux, int fd, iomux_callbacks_t *cbs);
void iomux_remove(iomux_t *iomux, int fd);
int  iomux_set_timeout(iomux_t *iomux, int fd, struct timeval *timeout);
int  iomux_schedule(iomux_t *iomux, struct timeval *timeout, iomux_cb_t cb, void *priv);
int  iomux_reschedule(iomux_t *iomux, struct timeval *timeout, iomux_cb_t cb, void *priv);
int  iomux_unschedule(iomux_t *iomux, iomux_cb_t cb, void *priv);
int  iomux_listen(iomux_t *iomux, int fd);
void iomux_loop_end_cb(iomux_t *iomux, iomux_cb_t cb, void *priv);
void iomux_hangup_cb(iomux_t *iomux, iomux_cb_t cb, void *priv);
void iomux_loop(iomux_t *iomux, int timeout);
void iomux_end_loop(iomux_t *iomux);
void iomux_run(iomux_t *iomux, struct timeval *timeout);
int  iomux_write(iomux_t *iomux, int fd, const void *buf, int len);
void iomux_close(iomux_t *iomux, int fd);
void iomux_destroy(iomux_t *iomux);

#ifdef __cplusplus
}
#endif

#endif

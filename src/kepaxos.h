#ifndef __CONSENSUS__H__
#define __CONSENSUS__H__
//
// Key-based Egalitarian Paxos
//

typedef enum {
    CMD_TYPE_SET,
    CMD_TYPE_DEL,
    CMD_TYPE_EVICT
} kepaxos_cmd_type_t;

typedef struct __kepaxos_cmd_s kepaxos_cmd_t;
typedef struct __kepaxos_s kepaxos_t;

typedef struct {
    int (*send)(char **recipients, int num_recipients, void *cmd, size_t cmd_len);
    int (*commit)(kepaxos_cmd_type_t cmd, void *key, size_t klen, void *data, size_t dlen);
    int (*recover)(char  *peer, void *key, size_t klen);
} kepaxos_callbacks_t;

kepaxos_t *kepaxos_context_create(char *dbfile,
                                  char **peers,
                                  int num_peers,
                                  kepaxos_callbacks_t *callbacks);

void kepaxos_context_destroy(kepaxos_t *ke);

int kepaxos_command_response(char *peer, void *response, size_t *response_len);

int kepaxos_run_command(kepaxos_t *ke,
                        char *peer,
                        kepaxos_cmd_type_t type,
                        void *key,
                        size_t klen,
                        void *data);

int kepaxos_received_command(kepaxos_t *ke, char *peer, void *cmd, size_t cmdlen);

#endif

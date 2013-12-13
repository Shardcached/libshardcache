#ifdef __cplusplus
extern "C" {
#endif

typedef struct __rbb_s rbb_t;

rbb_t *rbb_create(int size);
void rbb_skip(rbb_t *rbb, int size);
int rbb_read(rbb_t *rbb, u_char *out, int size);
int rbb_write(rbb_t *rbb, u_char *in, int size);
int rbb_len(rbb_t *rbb);
int rbb_find(rbb_t *rbb, u_char octet);
int rbb_read_until(rbb_t *rbb, u_char octet, u_char *out, int maxsize);
void rbb_clear(rbb_t *rbb);
void rbb_destroy(rbb_t *rbb);
const unsigned char * rbb_dump(rbb_t *rbb);

#ifdef __cplusplus
}
#endif

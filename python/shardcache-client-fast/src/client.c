#include <Python.h>
#include <shardcache_client.h>

typedef struct {
    PyObject_HEAD
    shardcache_client_t * shardcache;
} Client;

static PyObject * Client_new(PyTypeObject * type, PyObject * args, PyObject * kwds) {
    PyObject * self   = type->tp_alloc(type, 0);
    Client   * client = (Client *)self;

    if (self == NULL)
        return NULL;

    PyObject * node_list = NULL;
    PyObject * secret    = NULL;
    char     * auth      = NULL;

    Py_ssize_t node_list_nr = 0;
    if (!PyArg_ParseTuple(args, "Os", &node_list, &auth))
        goto fail;

    if (!PySequence_Check(node_list)) {
        // TODO: throw exceptions
        goto fail;
    }

    node_list_nr = PySequence_Size(node_list);
    if (node_list_nr <= 0)
        goto fail;

    shardcache_node_t **nodes = malloc(node_list_nr * sizeof(shardcache_node_t *));

    int i;
    for (i = 0; i < node_list_nr; i++) {
        PyObject * node_object = PySequence_GetItem(node_list, i);

        char * node_label   = NULL;
        char * node_address = NULL;

        PyArg_ParseTuple(node_object, "ss", &node_label, &node_address);

        nodes[i] = shardcache_node_create(node_label, &node_address, 1);
    }

    client->shardcache = shardcache_client_create(nodes, (int)node_list_nr, auth);
    shardcache_free_nodes(nodes, (int)node_list_nr);

    return self;

fail:
    Py_XDECREF(secret);
    Py_XDECREF(node_list);
    Py_XDECREF(self);
    return NULL;    
}

static void Client_dealloc(PyObject * self) {
    Client * client = (Client *)self;
    if (client->shardcache)
        shardcache_client_destroy(client->shardcache);

    self->ob_type->tp_free(self);
}

static PyObject * Client_get(PyObject * self, PyObject * args) {
    Client   * client     = (Client *)self;
    PyObject * key_string = NULL;
    if (!PyArg_ParseTuple(args, "O", &key_string))
        return NULL;

    void   * data;
    size_t   data_len;

    data_len = shardcache_client_get(client->shardcache,
                                     PyString_AsString(key_string),
                                     PyString_Size(key_string),
                                     &data);


    if (data_len == 0)
        // TODO: exception
        return NULL;

    PyObject * response = PyString_FromStringAndSize(data, data_len);
    free(data);
    return response;
}

static PyObject * Client_set(PyObject * self, PyObject * args) {
    Client   * client       = (Client *)self;
    PyObject * key_string   = NULL;
    PyObject * value_string = NULL;
    unsigned int expire = 0;

    if (!PyArg_ParseTuple(args, "OO|I", &key_string, &value_string, &expire))
        return NULL;

    int response = shardcache_client_set(client->shardcache,
                                         PyString_AsString(key_string),
                                         PyString_Size(key_string),
                                         PyString_AsString(value_string),
                                         PyString_Size(value_string),
                                         expire); // Hardcoded for now

    return Py_BuildValue("i", response);
}

/* - */

static PyMethodDef Client_methods[] = {
    { "get", Client_get, METH_VARARGS, "Get a key from the shardcache" },
    { "set", Client_set, METH_VARARGS, "Set a key in the shardcache" },
    { NULL }
};

static PyTypeObject ClientType = {
    PyObject_HEAD_INIT(NULL)
    0,                              // ob_size
    "shardcacheclient.Client",      // tp_name
    sizeof(Client),                 // tp_basicsize
    0,                              // tp_itemsize
    Client_dealloc,                 // tp_dealloc
    NULL,                           // tp_print
    NULL,                           // tp_getattr
    NULL,                           // tp_setattr
    NULL,                           // tp_compare
    NULL,                           // tp_repr
    NULL,                           // tp_as_number
    NULL,                           // tp_as_sequence
    NULL,                           // tp_as_mapping
    NULL,                           // tp_hash
    NULL,                           // tp_call
    NULL,                           // tp_str
    NULL,                           // tp_getattro
    NULL,                           // tp_setattro
    NULL,                           // tp_as_buffer
    Py_TPFLAGS_DEFAULT,             // tp_flags
    "Shardcache client interface",  // tp_doc
    NULL,                           // tp_traverse
    NULL,                           // tp_clear
    NULL,                           // tp_richcompare
    0,                              // tp_weaklistoffset
    NULL,                           // tp_iter
    NULL,                           // tp_iternext
    Client_methods,                 // tp_methods
    NULL,                           // tp_members
    NULL,                           // tp_getset
    NULL,                           // tp_base
    NULL,                           // tp_dict
    NULL,                           // tp_descr_get
    NULL,                           // tp_descr_set
    0,                              // tp_dictoffset
    NULL,                           // tp_init
    NULL,                           // tp_alloc
    Client_new,                     // tp_new
};

/* - */

static PyMethodDef module_methods[] = {
    { NULL }
};

PyMODINIT_FUNC initshardcacheclient(void) {
    PyObject * module;

    if (PyType_Ready(&ClientType) < 0)
        return;

    module = Py_InitModule("shardcacheclient", module_methods);
    if (module == NULL)
        return;

    Py_INCREF(&ClientType);
    PyModule_AddObject(module, "Client", (PyObject *)&ClientType);
}


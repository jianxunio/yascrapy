API Guide
====================================

yascrapy.base
_______________________

.. automodule:: yascrapy.base

.. autoclass:: BaseWorker
    :members:
    
    .. automethod:: __init__

.. autoclass:: BaseProducer
    :members:
    
    .. automethod:: __init__


yascrapy.request_queue
_______________________

.. automodule:: yascrapy.request_queue

.. autoclass:: Request
    :members:
    
    .. automethod:: __init__

.. autoclass:: RequestError
    :members:

    .. automethod:: __init__

.. autoclass:: RequestQueue
    :members:

    .. automethod:: __init__




yascrapy.response_queue
________________________

.. automodule:: yascrapy.response_queue
  
.. autoclass:: Response
    :members:

    .. automethod:: __init__

.. autoclass:: ResponseError
    :members:

    .. automethod:: __init__

.. autoclass:: ResponseQueue
    :members:

    .. automethod:: __init__


yascrapy.filter_queue
______________________

.. automodule:: yascrapy.filter_queue

.. autoclass:: FilterError
    :members:

    .. automethod:: __init__
    
.. autoclass:: FilterQueue
    :members:

    .. automethod:: __init__


yascrapy.config
___________________

.. automodule:: yascrapy.config

.. autoclass:: Config
    :members:

    .. automethod:: __init__



yascrapy.ssdb
______________________

.. automodule:: yascrapy.ssdb

    .. autofunction:: get_proxy_client
    .. autofunction:: get_clients
    .. autofunction:: get_client


yascrapy.bloomd
______________________

.. automodule:: yascrapy.bloomd

    .. autofunction:: get_client


yascrapy.rabbitmq
_____________________

.. automodule:: yascrapy.rabbitmq

    .. autofunction:: create_conn
REST API DOCUMENTATION
======================


ReST Resources and Methods
==========================

Resource: ClassRestExample
--------------------------

HTTP resource for testing class with instance methods

ROUTE: GET /ClassRestExample/_create
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Create a session-level object


**param** : val *int* --  --  some dummy value





ROUTE: POST /ClassRestExample/echo
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Some sort of doc

**param** : self {{string}} -- unique id of a created instance

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *str* --   sting based on state and params





ROUTE: GET /ClassRestExample/expire
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Release/close an instance on the server that was created through invocation of _create for the
associated resource


*undocumented param*: new_lease_time of type int
*undocumented param*: _uuid of type Optional



Resource: ClassRestExampleAsync
-------------------------------

HTTP resource for testing class with instance methods

ROUTE: GET /ClassRestExampleAsync/_create
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Create a session-level object


**param** : val *int* --  --  some dummy value





ROUTE: POST /ClassRestExampleAsync/echo
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Some sort of doc

**param** : self {{string}} -- unique id of a created instance

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *str* --   sting based on state and params





ROUTE: GET /ClassRestExampleAsync/expire
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Release/close an instance on the server that was created through invocation of _create for the
associated resource


*undocumented param*: new_lease_time of type int
*undocumented param*: _uuid of type Optional

**param** : self {{string}} -- unique id of a created instance



ROUTE: GET /RestAPIExample/api_get_basic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *str* --  -- 

**response**: *str* --   String for test_api_basic





ROUTE: GET /RestAPIExample/api_get_stream
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *int* --  [streamed]  stream of int





ROUTE: GET /RestAPIExample/api_get_stream_text
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *str* --  [streamed]  stream of int





ROUTE: POST /RestAPIExample/api_post_basic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/json

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *str* --   stream of int





ROUTE: POST /RestAPIExample/api_post_stream
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *str* --  -- 

**response**: *int* --  [streamed]  stream of int





ROUTE: POST /RestAPIExample/api_post_stream_text
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *str* --  [streamed]  stream of int





ROUTE: POST /RestAPIExample/api_post_streamed_req_and_resp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *iterator of type <class 'str'>* --  -- 

**response**: *str* --  [streamed]  stream of int





ROUTE: GET /RestAPIExample/publish_result
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>

*undocumented param*: result of type str



Resource: RestAPIExampleAsync
-----------------------------

HTTP resource for testing ReST examples, with all static methods

ROUTE: GET /RestAPIExampleAsync/_create
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>

*undocumented param*: val of type int



ROUTE: GET /RestAPIExampleAsync/api_get_basic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Some sort of doc

**param** : param1 *int* --  --  docs for first param

**param** : param2 *bool* --  --  docs for 2nd param

**param** : param3 *float* --  --  docs for param #3

**param** : param4 *str* --  --  docs for last param

**param** : param5 *dict of str, float* --  -- 

**response**: *str* --   String for test_api_basic


*undocumented param*: varargs of type Tuple



ROUTE: GET /RestAPIExampleAsync/api_get_stream
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *int* --  [streamed]  stream of int





ROUTE: GET /RestAPIExampleAsync/api_get_stream_bytes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**param** : param5 *Optional* --  -- 

**response**: *bytes* --  [streamed]  stream of int





ROUTE: POST /RestAPIExampleAsync/api_post_basic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/json

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *str* --   stream of int





ROUTE: POST /RestAPIExampleAsync/api_post_stream
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *str* --  -- 

**response**: *int* --  [streamed]  stream of int





ROUTE: POST /RestAPIExampleAsync/api_post_stream_text
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *str* --  [streamed]  stream of int





ROUTE: POST /RestAPIExampleAsync/api_post_streamed_req_and_resp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *iterator of type <class 'str'>* --  -- 

**response**: *str* --  [streamed]  stream of int





ROUTE: GET /RestAPIExampleAsync/expire
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Release/close an instance on the server that was created through invocation of _create for the
associated resource


*undocumented param*: new_lease_time of type int
*undocumented param*: _uuid of type Optional

**param** : self {{string}} -- unique id of a created instance



ROUTE: GET /RestAPIExampleAsync/explicit_constructor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>

*undocumented param*: val of type int



ROUTE: GET /RestAPIExampleAsync/my_value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>


**param** : self {{string}} -- unique id of a created instance



ROUTE: GET /RestAPIExampleAsync/my_value_repeated
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined
<<no documentation provided>>

*undocumented param*: count of type int

**param** : self {{string}} -- unique id of a created instance



ROUTE: GET /RestAPIExampleAsync/my_value_repeated_string
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined
<<no documentation provided>>

*undocumented param*: count of type int

**param** : self {{string}} -- unique id of a created instance



ROUTE: GET /RestAPIExampleAsync/publish_result
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>

*undocumented param*: result of type str



ROUTE: GET /RestAPIExampleAsync/raise_exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>




Resource: RestAPIExampleAsyncPost
---------------------------------

HTTP resource for testing ReST examples, with all static methods

ROUTE: GET /RestAPIExampleAsyncPost/_create
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>

*undocumented param*: val of type int



ROUTE: POST /RestAPIExampleAsyncPost/api_post_basic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Some sort of doc

**param** : param1 *int* --  --  docs for first param

**param** : param2 *bool* --  --  docs for 2nd param

**param** : param3 *float* --  --  docs for param #3

**param** : param4 *str* --  --  docs for last param

**param** : param5 *dict of str, float* --  -- 

**response**: *str* --   String for test_api_basic


*undocumented param*: varargs of type Tuple



ROUTE: POST /RestAPIExampleAsyncPost/api_post_stream
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *int* --  [streamed]  stream of int





ROUTE: POST /RestAPIExampleAsyncPost/api_post_stream_bytes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**param** : param5 *Optional* --  -- 

**response**: *bytes* --  [streamed]  stream of int





ROUTE: POST /RestAPIExampleAsyncPost/api_post_stream_text
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *str* --  [streamed]  stream of int





ROUTE: POST /RestAPIExampleAsyncPost/api_post_streamed_req_and_resp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *iterator of type <class 'str'>* --  -- 

**response**: *str* --  [streamed]  stream of int





ROUTE: GET /RestAPIExampleAsyncPost/expire
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Release/close an instance on the server that was created through invocation of _create for the
associated resource


*undocumented param*: new_lease_time of type int
*undocumented param*: _uuid of type Optional

**param** : self {{string}} -- unique id of a created instance



ROUTE: POST /RestAPIExampleAsyncPost/explicit_constructor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>

*undocumented param*: val of type int



ROUTE: POST /RestAPIExampleAsyncPost/my_value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>


**param** : self {{string}} -- unique id of a created instance



ROUTE: POST /RestAPIExampleAsyncPost/my_value_repeated
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined
<<no documentation provided>>

*undocumented param*: count of type int

**param** : self {{string}} -- unique id of a created instance



ROUTE: POST /RestAPIExampleAsyncPost/my_value_repeated_string
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined
<<no documentation provided>>

*undocumented param*: count of type int

**param** : self {{string}} -- unique id of a created instance



ROUTE: POST /RestAPIExampleAsyncPost/publish_result
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>

*undocumented param*: result of type str



Resource: RestAPIExampleAsyncPostInherited
------------------------------------------

HTTP resource for testing ReST examples, with all static methods

ROUTE: GET /RestAPIExampleAsyncPostInherited/_create
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>

*undocumented param*: val of type int



ROUTE: POST /RestAPIExampleAsyncPostInherited/api_post_basic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Some sort of doc

**param** : param1 *int* --  --  docs for first param

**param** : param2 *bool* --  --  docs for 2nd param

**param** : param3 *float* --  --  docs for param #3

**param** : param4 *str* --  --  docs for last param

**param** : param5 *dict of str, float* --  -- 

**response**: *str* --   String for test_api_basic





ROUTE: POST /RestAPIExampleAsyncPostInherited/api_post_stream
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *int* --  [streamed]  stream of int





ROUTE: POST /RestAPIExampleAsyncPostInherited/api_post_stream_bytes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**param** : param5 *Optional* --  -- 

**response**: *bytes* --  [streamed]  stream of int





ROUTE: POST /RestAPIExampleAsyncPostInherited/api_post_stream_text
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *str* --  [streamed]  stream of int





ROUTE: POST /RestAPIExampleAsyncPostInherited/api_post_streamed_req_and_resp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *iterator of type <class 'str'>* --  -- 

**response**: *str* --  [streamed]  stream of int





ROUTE: GET /RestAPIExampleAsyncPostInherited/expire
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Release/close an instance on the server that was created through invocation of _create for the
associated resource


*undocumented param*: new_lease_time of type int
*undocumented param*: _uuid of type Optional

**param** : self {{string}} -- unique id of a created instance



ROUTE: POST /RestAPIExampleAsyncPostInherited/explicit_constructor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>

*undocumented param*: val of type int



ROUTE: POST /RestAPIExampleAsyncPostInherited/my_value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>


**param** : self {{string}} -- unique id of a created instance



ROUTE: POST /RestAPIExampleAsyncPostInherited/my_value_repeated
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined
<<no documentation provided>>

*undocumented param*: count of type int

**param** : self {{string}} -- unique id of a created instance



ROUTE: POST /RestAPIExampleAsyncPostInherited/my_value_repeated_string
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined
<<no documentation provided>>

*undocumented param*: count of type int

**param** : self {{string}} -- unique id of a created instance



ROUTE: POST /RestAPIExampleAsyncPostInherited/my_value_repeated_string_disconnected
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/streamed; charset=x-user-defined
<<no documentation provided>>

*undocumented param*: count of type int

**param** : self {{string}} -- unique id of a created instance



ROUTE: POST /RestAPIExampleAsyncPostInherited/publish_result
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain
<<no documentation provided>>

*undocumented param*: result of type str



ROUTE: GET /RestAPIExampleErrorAsync/api_get_basic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/plain

Some sort of doc

**param** : param1 *int* --  --  docs for first param

**param** : param2 *bool* --  --  docs for 2nd param

**param** : param3 *float* --  --  docs for param #3

**param** : param4 *str* --  --  docs for last param

**param** : param5 *dict of str, float* --  -- 

**response**: *str* --   String for test_api_basic





ROUTE: GET /RestAPIExampleErrorAsync/api_post_basic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Content-Type: text/json

Some sort of doc

**param** : param1 *int* --  -- 

**param** : param2 *bool* --  -- 

**param** : param3 *float* --  -- 

**param** : param4 *Optional* --  -- 

**response**: *str* --   stream of int





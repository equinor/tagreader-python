---
sidebar_position: 5
---

# Performing raw queries

**Warning**: This is an experimental feature for advanced users

If the methods described above do not cover your needs, an experimental feature was introduced in Tagreader 2.4 that 
lets the user perform raw SQL queries. This is available for the two ODBC handlers (`pi` and `ip21`) as well as for 
`aspenone`.

Raw SQL queries can be performed on ODBC handlers by calling the client method `query_sql()` with the following input 
arguments.

* `query` : The query itself.
* `parse` (optional): Set to `True` to attempt to return a `pd.DataFrame`. If set to `False`, the return value will be 
* a `pyodbc.Cursor` object that can be iterated over to obtain query results. **Default**: `True`

Results from raw SQL queries are not cached.

The `query_sql()` method is also available for the `aspenone` handler with the following differences:

The '%' sign does not work. Also, no attempt is made at parsing the result regardless of the value of `parse`. 
Therefore set `parse=False` and do the json parsing of the resulting string in your application.

Queries are by default performed using ADSA, so there should be no need to specify a connection string. However, 
it is possible to do so by calling `c.handler.initialize_connectionstring()` (where `c` is your client object) 
with the following input arguments:

* `host` (optional): The path to the host.
* `port` (optional): The port. **Default**: 10014
* `connection_string` (optional): A complete connection string that will override any value for `host` and `port`.

If `connection_string` is not specified, a default connection string will be generated based on values for `host` 
and `port`.

The initialization needs only be done once. The resulting connection string can be inspected with 
`c.handler._connection_string`
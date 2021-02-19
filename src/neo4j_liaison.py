# Version 1.2 , last updated 1/6/2021

"""
    ----------------------------------------------------------------------------------
    MIT License

    Copyright (c) 2020-2021 Julian A. West

    This file is part of the "Brain Annex" project (https://BrainAnnex.org)

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
	----------------------------------------------------------------------------------
"""
import sys
from os import path

project_dir = path.dirname(__file__)      #   Example: "/home/juls/Documents/OC/platform"
#sys.path.append(project_dir + '/venv/lib/python3.8/site-packages')

from neo4j import GraphDatabase
from neo4j import __version__ as neo4j_driver_version


class Neo4jLiaison:
    """
    To access a Neo4j database.
    It provides a higher-level wrapper around the Neo4j python connectivity library "Neo4j Python Driver".
    Tested on version 4.0.6 of Neo4j Community Edition, with version 4.1.0 of the Neo4j Python Driver"

    Documentation: https://neo4j.com/docs/api/python-driver/current/api.html
    """

    def __init__(self, url: str, user: str, pwd: str):
        """

        :param url:     URL to connect to database with.  EXAMPLE: "neo4j://localhost:7687"
        :param user:    Username to connect to database with
        :param pwd:     Password to connect to database with
        """

        self._driver = None             # Object to connect to Neo4j's Bolt driver for Python
        self._current_session = None    # A neo4j.Session object

        try:
            self._driver = GraphDatabase.driver(url, auth=(user, pwd))  # Create a driver object
        except Exception as ex:
            error_msg = "CHECK IF NEO4J IS RUNNING! While instantiating the Neo4jLiaison object, failed to create the driver: " + str(ex)
            raise Exception (error_msg)



    @staticmethod
    def version() -> str:
        # Return the version of the Neo4j driver being used.  EXAMPLE: "4.1.0"
        return neo4j_driver_version



    def close(self) -> None:
        """
        Terminate the database connection.
        Note: this method is automatically invoked after the last operation of a "with" statement

        :return:    None
        """

        if self._driver is not None:
            self._driver.close()



    ###########################################################################
    #                                                                         #
    #                       SESSION-RELATED METHODS                           #
    #                                                                         #
    ###########################################################################

    def new_session(self):
        """
        Create, save and return a neo4j.Session object (used to run Cypher queries)

        :return:    A new neo4j.Session object
        """

        if self._driver is None:
            raise Exception("Calling the session() method, but self._driver isn't set")

        self._current_session = self._driver.session()  # Save the session for future use
        return self._current_session



    def get_session(self):
        """
        Retrieve, or possibly create, a "session" object

        :return:    A neo4j.Session object
        """

        if self._current_session is not None:
            return self._current_session
        else:
            return self.new_session()



    ###########################################################################
    #                                                                         #
    #                       METHODS TO RETRIEVE DATA                          #
    #                                                                         #
    ###########################################################################

    def retrieve_node_by_label_and_id(self, label: str, id_value: int) -> {}:
        """
        Return the record corresponding to a Neo4j node identified by the given label, and by
        an "id" attribute with a value as specified,
        within the context of the given session
        TODO: add a version that looks up the value of a single field

        EXAMPLE:
            record = conn.retrieve_node_by_id("OC_subject", 86)
            gender = record["gender"]

        :param label:       A string with a Neo4j label
        :param id_value:    An integer with a value to match an attribute named "id" in the nodes

        :return:            A dictionary with the record information (the node's attribute names are the keys), if found;
                            if not found, return None
        """

        sess = self.get_session()       # Retrieve or create a "session" object

        cypher = "MATCH (n:%s {id:$id}) RETURN n" % label   # Construct the Cypher string
        #print("In retrieve_node_by_label_and_id(). Cypher: " + cypher)

        result_obj = sess.run(cypher, id=id_value)   # A new neo4j.Result object
        # Alternate way:
        # result_obj = sess.run(cypher, {"id" : id})

        record = result_obj.single()    # Obtain the record from this result if available; else, return None
        #print("record: ", record)

        if record is None:
            return None

        node = record[0]    # Object of type neo4j.graph.Node
                            # EXAMPLE: <Node id=2273724 labels=frozenset({'OC_subject', 'OC'}) properties={'id': 110, 'gender': 'M', 'dob': '1-Jan-85'}>
                            # https://neo4j.com/docs/api/python-driver/current/api.html#node
        # Alternate way:
        # node = record["n"]

        node_as_items = node.items()    # An iterable of all property name-value pairs.  Type is:  <class 'dict_items'>
                                        # EXAMPLE:  dict_items([('id', 110), ('gender', 'M'), ('dob', '1-Jan-85')])

        dict_from_node = dict(node_as_items)    # Construct a dictionary from the contents of the iterable
                                                # Type shows as : <class 'dict'>
                                                # EXAMPLE: {'id': 110, 'gender': 'M', 'dob': '1-Jan-85'}

        return dict_from_node



    def retrieve_node_by_label_and_clause(self, label: str, clause: str) -> [{}]:
        """
        Return the records corresponding to all the Neo4j nodes with the specified label,
        and satisfying the given clause,
        within the context of the given session.
        The node referred to in the clause must be specified as "n."
        TODO: offer an option to specify a list of desired fields (e.g. "id", "name_short")

        If a more general lookup is needed, use query_list_multiple_fields_dict() instead.

        EXAMPLE:
            nodes = conn.retrieve_node_by_label_and_clause("OC_subject", "n.'gender'='M' AND n.'ht' > 70")

        :param label:   A string with a Neo4j label
        :param clause:  String with a clause to define the search; the node it refers to must be specified as "n."

        :return:        A list whose entries are dictionaries with each record's information (the node's attribute names are the keys)
        """

        sess = self.get_session()       # Retrieve or create a "session" object

        cypher = "MATCH (n:%s) WHERE %s RETURN n" % (label, clause)   # Construct the Cypher string
        print("In retrieve_node_by_label_and_clause(). Cypher query: ", cypher)

        # Run the query, which returns a "Neo4j result" object
        result_obj = sess.run(cypher)   # A new neo4j.Result object

        #print(list(result_obj))        # WARNING: this will "consume" the result object!

        # Turn the result into a list of dictionaries
        result_list = []
        for record in result_obj:
            #print("Record:", record)         # EXAMPLE:  <Record n=<Node id=2273663 labels=frozenset({'OC', 'OC_subject'}) properties={'gender': 'M', 'id': 49, 'ht': 77}>>
            node_object = record[0]           # Object of type neo4j.graph.Node
            #print("Node data:", node_object) # EXAMPLE:  <Node id=2273663 labels=frozenset({'OC', 'OC_subject'}) properties={'gender': 'M', 'id': 49, 'ht': 77}>

            node_as_items = node_object.items()     # An iterable of all property name-value pairs.  Type is:  <class 'dict_items'>
            #print(node_as_items)                   # EXAMPLE: dict_items([('gender', 'M'), ('id', 49), ('ht', 77)])

            dict_from_node = dict(node_as_items)    # Construct a dictionary from the contents of the iterable
            # Type shows as : <class 'dict'>
            #print(dict_from_node)                  # EXAMPLE: {'gender': 'M', 'id': 49, 'ht': 77}

            result_list.append(dict_from_node)

        #print(result_list)                         # Each entry in the list is a dictionary such as {'gender': 'M', 'id': 49, 'ht': 77}

        return result_list



    def retrieve_children(self, label, id_value, rel_name:str, order="") -> [{}]:
        """
        Retrieve all the children of a Neo4j node identified by the given label, and an "id" attribute with a value as specified,
        within the context of the given session

        :param label:       A string with a Neo4j label
        :param id_value:    A value to match an attribute named "id" in the node
        :param rel_name:    A string with the name of a relationship
        :param order:       An optional string. TODO: Not currently used

        :return:    A list of dictionaries - one list item per child node.
                    Each dictionary contains the record information of a node (the node's attribute names are the keys)
                    EXAMPLE:  [ {'collection_location': 'OpenCures', 'id': 190, 'date_collected': '27-Feb-20'},
                                {'collection_location': 'OpenCures', 'id': 62, 'date_collected': '31-May-19'} ]
        """
        sess = self.get_session()       # Retrieve or create a "session" object

        cypher = "MATCH (n:%s {id:$id})-[:%s]->(m) RETURN m" % (label, rel_name)
        print("In retrieve_children(): ", cypher)
        result_obj = sess.run(cypher, id=id_value, rel_name=rel_name)  # A new neo4j.Result object

        #print(result_obj)   # neo4j.work.result.Result object
        #print("Result converted to list: ", list(result_obj))
        # EXAMPLE: [<Record m=<Node id=2273968 labels=frozenset({'OC_sampling', 'OC'}) properties={'collection_location': 'OpenCures', 'id': 190, 'date_collected': '27-Feb-20'}>>,
        #           <Record m=<Node id=2273967 labels=frozenset({'OC_sampling', 'OC'}) properties={'collection_location': 'OpenCures', 'id': 62, 'date_collected': '31-May-19'}>>
        #          ]

        #print("Result keys: ", result_obj.keys())          # EXAMPLE: ['m']

        #print("Result value: ", result_obj.value())        # Returns a list of values
        # EXAMPLE:  [<Node id=2273968 labels=frozenset({'OC_sampling', 'OC'}) properties={'collection_location': 'OpenCures', 'id': 190,'date_collected': '27-Feb-20'}>,
        #            <Node id=2273967 labels=frozenset({'OC_sampling', 'OC'}) properties={'collection_location': 'OpenCures', 'id': 62, 'date_collected': '31-May-19'}>]

        result_as_list_dict = result_obj.data()             # Returns a list of dictionaries
        #print("Result data: ", result_as_list_dict)        # Returns a list of dictionaries
        # EXAMPLE:  [{'m': {'collection_location': 'OpenCures', 'id': 190, 'date_collected': '27-Feb-20'}},
        #            {'m': {'collection_location': 'OpenCures', 'id': 62, 'date_collected': '31-May-19'}}
        #           ]

        children = [i["m"] for i in result_as_list_dict]
        print("result: ", children)  # EXAMPLE:  [ {'collection_location': 'OpenCures', 'id': 190, 'date_collected': '27-Feb-20'},
                                     #             {'collection_location': 'OpenCures', 'id': 62, 'date_collected': '31-May-19'} ]
        return children



    def query_list_single_field(self, field_name: str, cypher: str, cypher_dict=None) -> []:
        """
        Run a given Cypher query that returns a list of values for the specified SINGLE field name,
        in the context of the current session

        EXAMPLE 1:
            cypher = "MATCH(n: OC_biomarker_type) RETURN n.classification AS classification"
            with conn.new_session():            # The "with" statement is optional
                result_list = conn.query_list("classification", cypher)

        EXAMPLE 2:
            cypher = "MATCH (n:OC_subject)" \
                     "WHERE n.username = $username AND n.passwd = $passwd " \
                     "RETURN n.id AS user_id"
            with conn.new_session():            # The "with" statement is optional
                result_list = self.conn.query_list("user_id", cypher, {"username": username, "passwd": passwd})

        IMPORTANT:  if no "AS" statement is used in the Cypher query, then the field name must be
                    spelled out in full (e.g. "n.id")

        :param field_name:  A string containing the name of the desired field (attribute)
        :param cypher:      A string containing a Cypher query.  Any name preceded by $ gets replaced by a value,
                                as specified in cypher_dict, below
        :param cypher_dict: Dictionary of data binding for the Cypher string.  EXAMPLE: {"subtype": "lipid"}

        :return:            A list of values for the requested field (attribute)
        """

        if cypher_dict is None:
            cypher_dict = {}

        sess = self.get_session()       # Retrieve or create a "session" object

        print("In query_list_single_field(). Cypher query: ", cypher)
        print("Cypher dictionary: ", cypher_dict)

        # Run the query, which returns a response object
        result_obj = sess.run(cypher, cypher_dict)     # A new neo4j.Result object
        # print(list(result_obj)) # [<Record n.name='unknown'>, <Record n.name='Sean'>, <Record n.name='Salu'>, <Record n.name='Ryan Kellogg'>, <Record n.name='Sherri'>, <Record n.name='unknown'>, <Record n.name='unknown'>, <Record n.name='Ron Primas'>, <Record n.name='Constance'>]

        # Turn the result into a list
        result_list = result_obj.value(field_name)

        # Alternate way:
        # result_list = [record[field_name] for record in result_obj]

        return result_list



    def query_list_multiple_fields_dict(self, cypher: str, cypher_dict=None) -> [{}]:
        """
        Run a given Cypher query that returns a list of dictionaries,
        in the context of the current session (which gets created if not already present.)

        Notes:
        In case of very few fields, a practical alternative is query_list_multiple_fields()
        If just doing a simple lookup by label and clause, may use retrieve_node_by_label_and_clause() instead.

        EXAMPLE:
            cypher = "MATCH(n: OC_biomarker_type) RETURN n.classification AS cls, n.subtype AS sub"
            with conn.new_session():    # The "with" statement is optional
                result_list = conn.query_list_multiple_fields_dict(cypher)

        :param cypher:      A string containing a Cypher query.  Any name preceded by $ gets replaced by a value,
                                as specified in cypher_dict, below
        :param cypher_dict: Dictionary of data binding for the Cypher string.  EXAMPLE: {"subtype": "lipid"}

        :return:            A list whose entries are dictionaries with each record's information
                            (the node's attribute names are the keys)
                            EXAMPLE:
                            [{'name': 'fatty acid', 'subtype': 'lipid'},
                             {'name': 'ultra long chain fatty acid', 'subtype': 'lipid'}
                            ]
        """

        if cypher_dict is None:
            cypher_dict = {}

        sess = self.get_session()       # Retrieve or create a "session" object

        print("In query_list_multiple_fields_dict(). Cypher query: ", cypher)
        print("Cypher dictionary: ", cypher_dict)

        # Run the query, which returns a "Neo4j result" object
        result_obj = sess.run(cypher, cypher_dict)     # A new neo4j.Result object

        # WARNING: the printing statement below will "consume" the result object!
        #print("Response object:" , list(result_obj)) # [<Record n.name='unknown'>, <Record n.name='Sean'>, <Record n.name='Salu'>, <Record n.name='Sherri'>, ..., <Record n.name='Constance'>]

        # Turn the result into a list of dictionaries
        """        
        for record in result_obj:
            print(record)           # Example: <Record cls='fatty acid' sub='lipid'>
       
        for record in result_obj:
            print(tuple(record))    # Example: ('fatty acid', 'lipid')
        
        for record in result_obj:
            print(dict(record))     # Example: {'cls': 'fatty acid', 'sub': 'lipid'}
        """

        result_list = [dict(record) for record in result_obj]
        #print(result_list)      # Example: [{'cls': 'fatty acid', 'sub': 'lipid'}, {'cls': 'ultra long chain fatty acid', 'sub': 'lipid'}]

        return result_list



    def query_list_multiple_fields(self, cypher: str, cypher_dict=None) -> [()]:
        """
        Run a given Cypher query that returns a list of tuple-valued entries,
        in the context of the current session (which gets created if not already present)

        Note: in case of a sizable numbers of fields, probably better to use query_list_multiple_fields_dict()

        EXAMPLE 1:
            cypher = "MATCH(n: OC_biomarker_type) RETURN n.classification, n.subtype"
            with conn.new_session():        # The "with" statement is optional
                result_list = conn.query_list_multiple_fields(cypher)

        EXAMPLE 2:
            cypher = "MATCH (n:OC_subject {id:$client_id})-[*3..6]->(r:OC_biomarker_result)-->(b:OC_biomarker)  " \
                     "RETURN b.name, r.value"
            with conn.new_session():       # The "with" statement is optional
                result_list = self.conn.query_list_multiple_fields(cypher, {"client_id": 110})

        :param cypher:      A string containing a Cypher query.  Any name preceded by $ gets replaced by a value,
                                as specified in cypher_dict, below
        :param cypher_dict: Dictionary of data binding for the Cypher string.  EXAMPLE: {"subtype": "lipid"}

        :return:            A list  of tuples
                            EXAMPLE: [(279.576, 'tgttc'), (4.09, 'negt446tc')]
        """

        if cypher_dict is None:
            cypher_dict = {}

        sess = self.get_session()       # Retrieve or create a "session" object

        print("In query_list_multiple_fields(). Cypher query: ", cypher)
        print("Cypher dictionary: ", cypher_dict)

        # Run the query, which returns a "Neo4j result" object
        result_obj = sess.run(cypher, cypher_dict)     # A new neo4j.Result object

        # WARNING: the printing statement below will "consume" the result object!
        #print("Response object:" , list(result_obj)) # [<Record n.name='unknown'>, <Record n.name='Sean'>, <Record n.name='Salu'>, <Record n.name='Sherri'>, ..., <Record n.name='Constance'>]

        # Turn the result into a list of tuples
        """
        for record in result_obj:
            print(record)           # Example: <Record r.value=279.576838838026 b.name='tgttc'>
        
        for record in result_obj:
            print(tuple(record))    # Example: (279.576838838026, 'tgttc')
        """

        result_list = [tuple(record) for record in result_obj]
        #print(result_list)      # Example: [(279.576838838026, 'tgttc'), (4.09113968515024, 'negt446tc')]

        return result_list



    def next_available_id(self, label: str, clause ="") -> int:
        """
        Restrict by node label and by optional clause (subquery)
        and return the next available value of the "id" attribute, treated as an Auto-Increment value.
        If no matches are found, return 1

        :param label:   String with the name of the desired node label
        :param clause:  Optional string to restrict the search.  EXAMPLE: "type:'soc', subtype:'post'"
        :return:        An integer with the next available ID
        """

        # Assemble the Cypher string
        if clause == "":
            cypher = F"MATCH (n:{label}) RETURN 1+max(n.id) AS max_value"
        else:
            cypher = "MATCH (n:%s {%s}) RETURN 1+max(n.id) AS max_value" % (label, clause)

        result_list = self.query_list_single_field("max_value", cypher)     # Returns a list with one single element
        # Note: if no node was matched in the query, the result of the 1+max will be None

        result = result_list[0]         # Extract the single element of the list
        #print("Next available ID: ", result)

        if result is None:
            return 1        # Arbitrarily use 1 as the first Auto-Increment value, if no other value is present

        return result



    ###############################################################################
    #                                                                             #
    #                           METHODS TO MODIFY DATA                            #
    #                                                                             #
    ###############################################################################

    def create_node(self, label: str, items: {}):
        """
        Create a new node with the given label and with attributes/values specified in the items dictionary

        :param label:   A string with a Neo4j label
        :param items:   A dictionary.  EXAMPLE: {'id': 123, 'gender': 'M'}

        :return:        A neo4j.Result object
        """

        #self.get_session()       # Retrieve or create a "session" object

        #print(F"In create_node(): label is `{label}` and items: {items}")

        # From the dictionary of attribute names/values, create a list of strings of the form 'attribute_name: $attribute_name'
        # The '$' characters designate the data binding to later perform
        pair_list = [F"{key}: ${key}" for key in items]
        #print("pair_list: ", pair_list)             # EXAMPLE:  ['id: $id', 'gender: $gender']

        # Merge all the strings in the list into a single comma-separated string
        separator_string = ", "
        attributes_str = separator_string.join(pair_list)   # EXAMPLE:   "id: $id, gender: $gender"
        #print("attributes_str: ", attributes_str)

        # Assemble the complete Cypher query
        cypher = "CREATE (:%s {%s})" % (label, attributes_str)
        #print("Cypher query: ", cypher)            # EXAMPLE: "CREATE (:OC_test {id: $id, gender: $gender})"

        # Run the Cypher query just created
        result = self.run_query(cypher, items)
        print("Result of creating new node:", result.value())

        return result



    def change_single_attribute_by_id(self, label: str, node_id: int, attribute_name: str, new_attribute_value: str) -> None:
        """
        Modify a single attribute value, in a node specified by its label and "id" attribute.
        In case of error, an Exception is thrown

        :param label:               A string with a Neo4j label
        :param node_id:             An integer to match to an attribute named "id" in the node
        :param attribute_name:      A string with the name of the attribute to modify (i.e. the key)
        :param new_attribute_value: A string with the value of the attribute to modify
        :return:                    None
        """

        cypher = F"MATCH (n:{label}) WHERE n.id = $node_id SET n.{attribute_name} = $new_attribute_value"   # Assemble the Cypher string
        print(F"In change_single_attribute_by_id(). Node id: {node_id} | cypher: `{cypher}` | new_attribute_value: `{new_attribute_value}`")

        cypher_dict = {"node_id": node_id, "new_attribute_value": new_attribute_value}
        result_obj = self.run_query(cypher, cypher_dict)    # A neo4j.Result object;
                                                            # more specifically, an object of type neo4j.work.result.Result
                                                            # See https://neo4j.com/docs/api/python-driver/current/api.html#neo4j.Result

        #print("Result: ", result_obj)   # It shows: <neo4j.work.result.Result object at 0x7fee788de250>
        #print("Result keys: ", result_obj.keys())   # It shows: []
        #print("Result value: ", result_obj.value())   # It shows: []
        #print("Result values: ", result_obj.values())   # It shows: []
        #print("Result data: ", result_obj.data())   # It shows: []

        #record = result_obj.single()
        #print("record:", record)                   # It shows: None
        #value = record.value()                     # Cannot be carried out on a None
        #info = result_obj.consume()

        return



    ###################################################################################
    #                                                                                 #
    #                       METHODS TO RUN GENERIC QUERIES                            #
    #                                                                                 #
    ###################################################################################

    def run_query(self, cypher: str, cypher_dict=None):
        """
        Run a general Cypher query

        :param cypher:      A string containing a Cypher query, possibly with some substrings such a "$node_id", indicating data binding
        :param cypher_dict: EXAMPLE, assuming that the cypher string contains the substrings "$node_id" and "$attribute_value":
                                        {'node_id': 20, 'attribute_value': 'My value'}

        :return:            A neo4j.Result object.  See https://neo4j.com/docs/api/python-driver/current/api.html#neo4j.Result
        """
        sess = self.get_session()       # Retrieve or create a "session" object

        if cypher_dict is None:
            cypher_dict = {}

        print("In run_query().  Cypher query: ", cypher)
        print("Cypher dictionary: ", cypher_dict)

        result_obj = sess.run(cypher, cypher_dict)     # A new neo4j.Result object

        return result_obj



    def query_NOT_IN_USE(self, query, db=None):
        # Execute a general Cypher query (NOT YET TESTED)  TODO: look into this variant
        assert self._driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            if db is not None:
                session = self._driver.sess(database=db)
            else:
                self._driver.sess()

            response = list(session.run(query))

        except Exception as ex:
            print("Query failed:", ex)

        finally:
            if session is not None:
                session.close()

        return response

# END class "Neo4jLiaison"

#!/usr/bin/env python3

import esprima
import glob
import json
import json2parquet as j2p
import multiprocessing
import pandas as pd
import sys

from os import path

################################################################################
# CONFIG PARAMETERS

# Top directory for all data and resource files
DATATOP = '/mnt/Data/UCOSP_DATA'

# JS source file directory
#JS_SOURCE_FILES = path.join(DATATOP, 'js_source_files')
JS_SOURCE_FILES = path.join(DATATOP, 'larger_sample_js_source_files/')
#JS_SOURCE_FILES = path.join(DATATOP, 'sample_js_source_files')

# Directory for url:filename dictionary files (PARQUET!)
URL_FILENAME_DICT = path.join(DATATOP, 'resources/full_url_list_parsed/')

# Output directory
OUTPUT_DIR = path.join(DATATOP, 'resources/symbol_counts/')

OUTPUT_FILE = 'full_run_trial2'
OUTPUT_FAIL = 'fails'

# Symbol list
SYM_LIST = 'master_sym_list.json'

# Number of workers:
WORKERS = multiprocessing.cpu_count()

################################################################################
class SymbolNode:

    def __init__(self, depth, width, parent_depth, parent_width): #, num_of_children):
        self._depth = depth
        self._width = width
        self._parent_depth = parent_depth
        self._parent_width = parent_width
        #self._num_of_children = num_of_children

    def setDepthWidth(depth, width):
        self._depth = depth
        self._width = width

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):

        # Symbol node class
        if isinstance(obj, SymbolNode):
            return{ "depth" : obj._depth,
                    "width" : obj._width,
                    "parent_depth": obj._parent_depth,
                    "parent_width": obj._parent_width}

        return json.JSONEncoder.default(self, obj)

################################################################################
class Element:

    ## Define keys you want to skip over
    BLACKLISTEDKEYS = ['parent']

    ## Constructor
    def __init__(self, esprima_ast):
        self._ast = esprima_ast         # Assign member var AST
        self._visitors = []             # Init empty visitor array


    ## Add a new visitor to execute (will be executed at each node)
    def accept(self, visitor):
        self._visitors.append(visitor)


    ## (private) Step through the node's queue of potential nodes to visit
    def _step(self, node, queue, depth, width):
        before = len(queue)

        for key in node.keys():         # Enumerate keys for possible children
            if key in self.BLACKLISTEDKEYS:
                continue                # Ignore node if it is blacklisted

            child = getattr(node, key)  # Assign child = node.key

            # if the child exists && the child has an attribute 'type'
            if child and hasattr(child, 'type') == True:
                child.parent = node     # Assign this node as child's parent
                child.parent_depth = depth #TODO: assign info
                child.parent_width = width #TODO: assign info
                queue.append(child)     # Append the child in this node's queue

            # if there is a list of children
            if isinstance(child, list):
                for item in child:      # Iterate through them and do the same
                                        #   as above
                    if hasattr(item, 'type') == True:
                        item.parent = node
                        item.parent_depth = depth #TODO: assign info
                        item.parent_width = width #TODO: assign info
                        queue.append(item)

        return len(queue) - before     # Return whether any children were pushed

    ## Walk through this AST
    def walk(self, api_symbols, filename):
        queue = [self._ast]             # Add the imported AST to the queue

        # TODO: initialize these entries
        for node in queue:
            node.parent_depth = 0
            node.parent_width = 0

        # TODO: v1 of depth and width counting
        depth                   = 0     # what level of the tree we are in
        width                   = 0     # how far from first node on this level we are
        this_depth_num_nodes    = 1     # how many nodes in this level are left
        next_depth_num_nodes    = 0     # how many nodes in the next level
        node_counter            = 0     # how many total nodes have been visited
        this_depth_count        = 0     # how many nodes are on this level (tot)

        # storage for the data
        extended_symbol_counter = {}
        symbol_counter          = {key: 0 for key in api_symbols}
        node_dict               = {key: [] for key in api_symbols}

        extended_symbol_counter['script_url_filename'] = filename

        while len(queue) > 0:           # While stuff in the queue
            node = queue.pop(0)          # Pop stuff off of the FRONT (0)
            this_depth_num_nodes -= 1   #TODO: reduce how many left
            node_counter += 1           #TODO: increment counter
            width = node_counter - this_depth_count - 1

            for v in self._visitors:    # Run visitor instances here
                result = v.visit(node, api_symbols)
                if result:
                    if result not in extended_symbol_counter.keys():
                        extended_symbol_counter[result] = 1;
                    else:
                        extended_symbol_counter[result] += 1;

                    #MemberExpression
                    if 'MemberExpression' == node.type:
                        tmp = node.property.name

                    # CallExpression
                    if 'CallExpression' == node.type:
                        tmp = node.callee.name

                    symbol_counter[tmp] += 1 # increment counter
                    this_node = SymbolNode(depth, width, node.parent_depth, node.parent_width)
                    node_dict[tmp].append(this_node)
                    break


            # If node is an instance of "esprima node", step through the node
            #   Returns how many children have been added to the queue
            if isinstance(node, esprima.nodes.Node):

                # Feed the nodes that will be labeled as children the current
                #   depth and width
                next_depth_num_nodes += self._step(node, queue, depth, width)

                #if (node.type == "MemberExpression"):
                #try:
                #    print("---> {} = {}".format(node.property.type, node.property.name))
                #except AttributeError as e:
                #    pass
                #try:
                #    print("---> {} = {}".format(node.callee.type, node.property.name))
                #except AttributeError as e:
                #    pass


            # Once this tree depth has been walked, update with the existing
            #   "next" set and reset the next set to 0. Increment depth by 1,
            #   and keep a tally on how many nodes have been counted up until
            #   this depth.
            if this_depth_num_nodes == 0:
                this_depth_num_nodes = next_depth_num_nodes # update current list
                next_depth_num_nodes = 0                    # reset this list
                this_depth_count = node_counter             #
                depth += 1
                #print("\n-------------------- Depth: {};\t Current: {};\t Width: {}\n\n".format(
                #    depth, this_depth_count, this_depth_num_nodes))

        print("\nDONE. Total stats:\n" + "-"*80)
        print("Tree depth:\t{}\nTotal nodes:\t{}\n".format(
                                            depth, node_counter) + "-"*80)



        return symbol_counter, extended_symbol_counter, node_dict


################################################################################
"""
Executes specified code given that an input node matches the property name of
    this node.

Attributes:
    _property_name: the name of the property required to execute the handler
    _node_handler:  code to execute if _property_name matches
    visit(node):    checks if input node's property matches this nodes; if yes,
                        executes the code passed into _node_handler, passing the
                        input node as an argument
"""
class MatchPropertyVisitor:

    ## Constructor
    def __init__(self, property_name, memb_expr_handler, call_expr_handler):
        self._property_name = property_name # userAgent, getContext, etc
        self._memb_expr_handler = memb_expr_handler #TODO: deprecated
        self._call_expr_handler = call_expr_handler #TODO: deprecated

    ##################################################
    def _recursive_check_objects(self, node, api_symbols):
        if node.object:
            return self._recurrance_visit(node.object, api_symbols)
        return False

    ## Visit the nodes, check if matches, and execute handler if it does
    def _recurrance_visit(self, node, api_symbols):

        # No more objects to look through
        if 'Identifier' == node.type:
            if node.name in api_symbols:
                return node.name

        # MemberExpression; maybe more objects
        elif 'MemberExpression' == node.type:
            if node.property.name in api_symbols:
                #self._memb_expr_handler(node)

                return_val = node.property.name
                tmp = self._recursive_check_objects(node, api_symbols)

                if tmp:
                    return_val = tmp + '.' + return_val

                return return_val

        # CallExpression; maybe more objects
        elif 'CallExpression' == node.type:
            if node.callee.name in api_symbols:
                #self._call_expr_handler(node)

                return_val = node.callee.name
                tmp = self._recursive_check_objects(node.callee, api_symbols)

                if tmp:
                    return_val = tmp + '.' + return_val

                return return_val
        return False
    ##################################################

    def _filter_parent_API(self, arg):
        if len(arg.split('.')[0]) == 1:
            arg = arg.split('.')
            arg.pop(0)
            arg = '.'.join(arg)
        return arg

    # FIRST VISIT: visit node, check if matches, and check for objects
    def visit(self, node, api_symbols):

        #MemberExpression
        if 'MemberExpression' == node.type:
            if node.property.name == self._property_name:
                #self._memb_expr_handler(node) #TODO: deprecated

                return_val = node.property.name
                tmp = self._recursive_check_objects(node, api_symbols)

                if tmp:
                    return_val = tmp + '.' + return_val
                    return_val = self._filter_parent_API(return_val)

                return return_val

        # CallExpression
        if 'CallExpression' == node.type:
            if node.callee.name == self._property_name:
                #self._call_expr_handler(node) #TODO: deprecated

                return_val = node.callee.name
                tmp = self._recursive_check_objects(node.callee, api_symbols)

                if tmp:
                    return_val = tmp + '.' + return_val
                    return_val = self._filter_parent_API(return_val)

                return return_val
        return False

#TODO: deprecated
def memb_expr_handler(n):

    def parent_type(node):
        return getattr(getattr(node, 'parent', None), 'type', None)

    print("- {}\t\tnode type:{} parent type:{}".format(n.property.name, n.type, parent_type(n)))

    if hasattr(n, 'parent_depth'):
        print("p-depth: {}, p-width: {}".format(n.parent_depth, n.parent_width))

#TODO: deprecated
def call_expr_handler(n):

    def parent_type(node):
        return getattr(getattr(node, 'parent', None), 'type', None)

    print("- {}\t\tnode type:{} parent type:{}".format(n.callee.name, n.type, parent_type(n)))

    if hasattr(n, 'parent_depth'):
        print("p-depth: {}, p-width: {}".format(n.parent_depth, n.parent_width))



################################################################################
# Extract JSON and JavaScript AST data from precompiled list
def importData(js_file):



    return ast, api_list


################################################################################
def uniquifyList(seq, idfun=None):
   # order preserving
   if idfun is None:
       def idfun(x): return x
   seen = {}
   result = []
   for item in seq:
       marker = idfun(item)
       if marker in seen: continue
       seen[marker] = 1
       result.append(item)
   return result


################################################################################
def worker_process(input_file):

    filename = input_file.split('/')[-1]

    # Try getting the AST using esprima, bail if non-JS syntax
    try:
        with open(input_file) as f:
            ast = esprima.parseScript(f.read())

    except esprima.error_handler.Error as e:
        print("Failure: non-javascript syntax detected. Terminating...")
        return False, filename

    #print("Success. Walking through the AST...")

    # Create an element using that AST
    el = Element(ast)
    for entry in api_symbols:
        visitor = MatchPropertyVisitor(entry, memb_expr_handler, call_expr_handler)
        el.accept(visitor)

    # Walk down the AST (breadth-first)
    symbol_counter, extended_symbol_counter, node_dict = el.walk(api_symbols, filename)

    return True, extended_symbol_counter

################################################################################
if __name__ == '__main__':

    print("Initialized to use {} workers.".format(WORKERS))

    # Extract all symbols from the generated "Symbols of Interest" list,
    #   and flatten all api symbols into a single list (for efficiency)
    with open(SYM_LIST, encoding='utf-8') as f:
        api_list = json.loads(f.read())

    print("Looking in \'{}\' for the API list...".format(SYM_LIST))
    api_symbols = [val for sublist in api_list.values() for val in sublist];
    api_symbols = uniquifyList(api_symbols)
    print("Success.")

    # Get file list from data directory
    print("Looking in \'{}\' for all .txt files...".format(JS_SOURCE_FILES))
    file_list = glob.glob(JS_SOURCE_FILES + "/*")
    print("Success. Found {} files.".format(len(file_list)))
    print("Begin iterating over the files to get symbol info.")
    print("-" * 80)

    # Storage arrays
    symbol_counts = []
    fails_list = []

    # Callback
    def log_result(result):
        if result[0]:
            symbol_counts.append(result[1])
        else:
            fails_list.append(result[1])

    # Setup thread pool
    for filename in file_list:
        log_result(worker_process(filename))

    # Done counting symbols
    print("All files done. Saving...")
    df = pd.DataFrame(symbol_counts)

    # Saving
    #with open(OUTPUT_DIR + OUTPUT_FILE + ".json", 'w') as f:
    #    output = json.dumps(json.loads(df.to_json(orient='index')),indent=2)
    #    f.write(output)

    df.to_parquet(OUTPUT_DIR + OUTPUT_FILE + '.parquet.gzip', compression='gzip')

    with open(OUTPUT_DIR + OUTPUT_FAIL + '.txt', 'w') as f:
        for item in fails_list:
            f.write("%s\n" % item)


    print("Success.\n\nDONE SCRIPT!")


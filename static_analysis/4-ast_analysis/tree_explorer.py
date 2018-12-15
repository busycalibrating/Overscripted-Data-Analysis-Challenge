#!/usr/bin/env python3

import esprima
import json
import json2parquet as j2p
import sys
#import visitor # Not yet in use

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
    def walk(self, api_symbols):
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
        symbol_counter          = {key: 0 for key in api_symbols}
        extended_symbol_counter = {}
        node_dict               = {key: [] for key in api_symbols}

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

                    print("\t-> Node counter: {}; width: {}".format(node_counter, width))
                    #MemberExpression
                    if 'MemberExpression' == node.type:
                        tmp = node.property.name

                    # CallExpression
                    if 'CallExpression' == node.type:
                        tmp = node.callee.name

                    symbol_counter[tmp] += 1 # increment counter
                    this_node = SymbolNode(depth, width, node.parent_depth, node.parent_width)
                    node_dict[tmp].append(this_node)
                    break # TODO: Not sure why this is double counting


            # If node is an instance of "esprima node", step through the node
            #   Returns how many children have been added to the queue
            if isinstance(node, esprima.nodes.Node):

                # Feed the nodes that will be labeled as children the current
                #   depth and width
                next_depth_num_nodes += self._step(node, queue, depth, width)
                print("{} : {}".format(this_depth_num_nodes, next_depth_num_nodes))

                #if (node.type == "MemberExpression"):
                try:
                    print("---> {} = {}".format(node.property.type, node.property.name))
                except AttributeError as e:
                    pass

                try:
                    print("---> {} = {}".format(node.callee.type, node.property.name))
                except AttributeError as e:
                    pass


            #TODO: Depth seemingly not working properly?
            # Once this tree depth has been walked, update with the existing
            #   "next" set and reset the next set to 0. Increment depth by 1,
            #   and keep a tally on how many nodes have been counted up until
            #   this depth.
            if this_depth_num_nodes == 0:
                this_depth_num_nodes = next_depth_num_nodes # update current list
                next_depth_num_nodes = 0                    # reset this list
                this_depth_count = node_counter             #
                depth += 1
                print("\n-------------------- Depth: {};\t Current: {};\t Width: {}\n\n".format(
                    depth, this_depth_count, this_depth_num_nodes))

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
        self._memb_expr_handler = memb_expr_handler #TODO: delete
        self._call_expr_handler = call_expr_handler #TODO: delete

    ##################################################
    def _recursive_check_objects(self, node, api_symbols):

        if node.object:
            tmp = self._recurrance_visit(node.object, api_symbols)
            print("\t\t\t\t\t\t\t{}".format(node.object.type))
            print(tmp)
            return tmp

        else:
            print("\t\t\t\t\t\t\tNO OBJECT FOUND!")

        return False

    ## Visit the nodes, check if matches, and execute handler if it does
    def _recurrance_visit(self, node, api_symbols):
        #property_full_name = property_name # to deal with namespace stuff

        # No more objects to look through
        if 'Identifier' == node.type:
            if node.name in api_symbols:
                return node.name

        # MemberExpression; maybe more objects
        elif 'MemberExpression' == node.type:
            if node.property.name in api_symbols:
                self._memb_expr_handler(node)

                print("LALALALALALA")
                return_val = node.property.name
                tmp = self._recursive_check_objects(node, api_symbols)

                if tmp:
                    return_val = tmp + '.' + return_val

                return return_val

        # CallExpression; maybe more objects
        elif 'CallExpression' == node.type:
            if node.callee.name in api_symbols:
                self._call_expr_handler(node)

                return_val = node.callee.name
                tmp = self._recursive_check_objects(node.callee, api_symbols)

                if tmp:
                    return_val = tmp + '.' + return_val

                return return_val

        return False
    ##################################################

    # Visit the nodes, check if matches, and execute handler if it does
    def visit(self, node, api_symbols):
        #property_full_name = property_name # to deal with namespace stuff

        #MemberExpression
        if 'MemberExpression' == node.type:
            if node.property.name == self._property_name:
                self._memb_expr_handler(node)

                return_val = node.property.name
                tmp = self._recursive_check_objects(node, api_symbols)

                if tmp:
                    return_val = tmp + '.' + return_val

                print(">\t>\t>\t>\t>\t{}\t<\t<\t<\t<\t<\t".format(return_val))

                return return_val

        # CallExpression
        if 'CallExpression' == node.type:
            if node.callee.name == self._property_name:
                self._call_expr_handler(node)

                return_val = node.callee.name
                tmp = self._recursive_check_objects(node.callee, api_symbols)

                if tmp:
                    return_val = tmp + '.' + return_val

                print(">\t>\t>\t>\t>\t{}\t<\t<\t<\t<\t<\t<".format(return_val))

                return return_val

        return False

# Probably can cut this...
def memb_expr_handler(n):

    def parent_type(node):
        return getattr(getattr(node, 'parent', None), 'type', None)

    print("- {}\t\tnode type:{} parent type:{}".format(n.property.name, n.type, parent_type(n)))

    if hasattr(n, 'parent_depth'):
        print("p-depth: {}, p-width: {}".format(n.parent_depth, n.parent_width))

def call_expr_handler(n):

    def parent_type(node):
        return getattr(getattr(node, 'parent', None), 'type', None)

    print("- {}\t\tnode type:{} parent type:{}".format(n.callee.name, n.type, parent_type(n)))

    if hasattr(n, 'parent_depth'):
        print("p-depth: {}, p-width: {}".format(n.parent_depth, n.parent_width))



################################################################################
# Extract JSON and JavaScript AST data from precompiled list
def importData():

    if (len(sys.argv) == 2):
        with open(sys.argv[1], encoding='utf-8') as data_file:
            api_list = json.loads(data_file.read())
        ast = esprima.parseScript(open('js/snowplow.js').read())

        return ast, api_list

    elif (len(sys.argv) == 3):
        with open(sys.argv[1], encoding='utf-8') as data_file:
            api_list = json.loads(data_file.read())
        with open(sys.argv[2]) as js_file:
            ast = esprima.parseScript(js_file.read())

        return ast, api_list

    else:
        print('''Warning: invalid input type!
Syntax:
        $ python3.6 this_script.py <path/to/api_list.json>
OR
        $ python3.6 this_script.py <path/to/api_list.json> \
<path/to/javascript.js>
''')
        exit()

    return


################################################################################
def uniquifyList(seq, idfun=None):
   # order preserving
   if idfun is None:
       def idfun(x): return x
   seen = {}
   result = []
   for item in seq:
       marker = idfun(item)
       # in old Python versions:
       # if seen.has_key(marker)
       # but in new ones:
       if marker in seen: continue
       seen[marker] = 1
       result.append(item)
   return result


################################################################################
def main():
    print ("#" * 100)

    # Get the AST using esprima
    ast, api_list = importData()

    api_symbols = [val for sublist in api_list.values() for val in sublist];
    api_symbols = uniquifyList(api_symbols)

    # Create an element using that AST
    el = Element(ast)

    for entry in api_symbols:
        visitor = MatchPropertyVisitor(entry, memb_expr_handler, call_expr_handler)
        el.accept(visitor)

    # Flatten all api symbols into a single list

    symbol_counter, extended_symbol_counter, node_dict = el.walk(api_symbols)

    with open('output_data/symbol_counts.json', 'w') as out1:
        json.dump(symbol_counter, out1, indent = 4)

    with open('output_data/extended_symbol_counts.json', 'w') as out2:
        json.dump(extended_symbol_counter, out2, indent = 4)

    with open('output_data/symbol_node_info.json', 'w') as out3:
        json.dump(node_dict, out3, indent=4, cls=CustomEncoder)

if __name__ == '__main__':
    main()

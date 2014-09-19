#ifndef TRIPLEBITQUERYGRAPH_H_
#define TRIPLEBITQUERYGRAPH_H_

//---------------------------------------------------------------------------
// TripleBit
// (c) 2011 Massive Data Management Group @ SCTS & CGCL. 
//     Web site: http://grid.hust.edu.cn/triplebit
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------

#include "IRepository.h"
#include "SPARQLLexer.h"
#include "SPARQLParser.h"
#include <vector>
#include <utility>

class TripleBitQueryGraph {

public:
    /// Possible duplicate handling modes
    enum DuplicateHandling { AllDuplicates, CountDuplicates, ReducedDuplicates, NoDuplicates, ShowDuplicates };
    //join variable's style
    enum JoinGraph{ CYCLIC,	ACYCLIC };

    typedef unsigned int ID;
	typedef unsigned int TripleNodeID;
	typedef unsigned int JoinVariableNodeID;
	typedef unsigned int ConstElementNodeID;

   	///A triple node in the graph
	//TODO the binding to bitmat ,may be use a enum
	typedef struct TripleNode {
		ID subject, predicate, object;
	    // Which of the three values are constants?
	    bool constSubject,constPredicate,constObject;
		enum Op{FINDSBYPO, FINDOBYSP, FINDPBYSO, FINDSBYP, FINDOBYP, FINDPBYS,FINDPBYO, FINDSBYO, FINDOBYS, FINDS, FINDP, FINDO,
	    	FINDSPBYO, FINDSOBYP, FINDPOBYS, FINDPSBYO, FINDOSBYP, FINDOPBYS,
	    	FINDSOBYNONE, FINDOSBYNONE, FINDSPBYNONE, FINDPSBYNONE, FINDOPBYNONE, FINDPOBYNONE,
	    	FINDSPO, NOOP
		};

	    TripleNodeID tripleNodeID;
		/// define the first scan operator
		Op scanOperation;
		int selectivity;
	    // Is there an implicit join edge to another TripleNode?
		TripleNode() { selectivity = -1; }
	    bool canJoin(const TripleNode& other) const;
	}tp_node;


	/// The variable node of triple node.
	typedef struct JoinVariableNode {
		//used to identify the join operation type
		enum JoinType{
			SS,
			OO,
			SO,
			PP,
			OP,
			SP,
			PS,
			PO,
			OS,
			UNKNOWN
		};
	    //not use!
		std::string text;
	    //the value in the triple node
	    ID value;
	    //the variable appear in which triple node and the triples dimension.
		enum DimType{SUBJECT = 1,PREDICATE = 2,OBJCET = 4};
	    std::vector<std::pair<TripleNodeID,DimType> > appear_tpnodes;
	   // Is there an variable edge to another JoinVariableNode
	    bool hasEdge(const JoinVariableNode& other)const;
	}jvar_node;

	/// A value filter
	struct Filter {
		/// The id
		unsigned id;
		/// The valid values. Sorted by id.
		std::vector<unsigned> values;
		/// Negative filter?
		bool exclude;
	};

	/// A (potentially) complex filter. Currently very limited.
	struct ComplexFilter {
		/// The ids
		unsigned id1,id2;
		/// Test for  equal?
		bool equal;
	};

	/// The same Element(s,p,o) between TripleNodes.
	//TODO:Not use now
	typedef struct ConstElementNode {
		enum Type { SUBJECT, PREDICATE, OBJECT };
		ID value;
		Type type;
	}const_e_node;


	/// Join Edge between two TripleNodes.
	typedef struct TripleNodesEdge {
	      /// The endpoints
		  TripleNodeID from,to;
	      /// Common variables
	      std::vector<ID> common;
	      /// Constructor
	      TripleNodesEdge(TripleNodeID from, TripleNodeID to, const std::vector<ID>& common);
	      /// Destructor
	      ~TripleNodesEdge();
	}tpn_edge;


	/// an edge between two variable nodes
	typedef struct JoinVariableNodesEdge {
		JoinVariableNodeID from;
		JoinVariableNodeID to;

		//JoinVariableNodesEdge(JoinVariableNodeID& from ,JoinVariableNodeID& to);
		//~JoinVariableNodesEdge();
	}j_var_edge;


	/// an edge between a join variable node and a triple node.
	typedef struct JoinVariableNodeTripleNodeEdge {
		JoinVariableNode from;
		TripleNode to;
		enum DimType{SUBJECT= 0,PREDICATE = 1,OBJCET = 2};
		DimType dimType;
		//TODO
		JoinVariableNodeTripleNodeEdge(JoinVariableNode& from, TripleNode& to, DimType dimType);
		~JoinVariableNodeTripleNodeEdge();
	}jvarn_tpn_edge;

   /// Description of a subquery
   struct SubQuery {
	  // The TripleNodes
	  std::vector<TripleNode> tripleNodes;
	  // The triple node's edges
	  std::vector<TripleNodesEdge> tripleEdges;

	  //the join Variable Node
	  std::vector<JoinVariableNodeID > joinVariables;
	  std::vector<JoinVariableNode> joinVariableNodes;
	  //the join Variable Edge.
	  std::vector<JoinVariableNodesEdge> joinVariableEdges;

	  //not use!! the join variable and the triple node edge.
	  std::vector<JoinVariableNodeTripleNodeEdge> joinVriableNodeTripleNodeEdge;

	  //TODO not implement!!!
	  /// The filter conditions
	  std::vector<Filter> filters;
	  /// The complex filter conditions
	  std::vector<ComplexFilter> complexFilters;
	  /// Optional subqueries
	  std::vector<SubQuery> optional;
	  /// Union subqueries
	  std::vector<std::vector<SubQuery> > unions;
	  /// tree root node
	  JoinVariableNodeID rootID;
	  /// leaf nodes
	  std::vector<JoinVariableNodeID> leafNodes;
	  /// is cyclic or acyclic
	  JoinGraph joinGraph;
	  /// selectivity;
	  std::map<JoinVariableNodeID,int> selectivityMap;
   };

private:
	/// The query itself,also is the Graph triple
	SubQuery query;
	/// The projection
	std::vector<ID> projection;
	std::vector<unsigned> predicateFlag;
	bool hasPredicate;
	/// The duplicate handling
	DuplicateHandling duplicateHandling;
	/// Maximum result size
	unsigned int limit;
	/// Is the query known to produce an empty result?
	bool knownEmptyResult;


public:
	///constructor
	 TripleBitQueryGraph();
	 virtual ~TripleBitQueryGraph();

	/// Clear the graph
	 void clear();
	 /// Construct the edges for a specific subquery(always the graph pattern ,option pattern,filter pattern join edges)
	 void constructSubqueryEdges();

	 /// Set the duplicate handling mode
	 void setDuplicateHandling(DuplicateHandling d) { duplicateHandling=d; }
	 /// Get the duplicate handling mode
	 DuplicateHandling getDuplicateHandling() const { return duplicateHandling; }
	 /// Set the result limit
	 void setLimit(unsigned int l) { limit=l; }
	 /// Get the result limit
	 unsigned getLimit() const { return limit; }
	 /// Known empty result
	 void markAsKnownEmpty() { knownEmptyResult=true; }
	 /// Known empty result?
	 bool knownEmpty() const { return knownEmptyResult; }

	 void Clear();
	 /// Get the query
	 SubQuery& getQuery() { return query; }
	 /// Get the query
	 const SubQuery& getQuery() const { return query; }

	 /// Add an entry to the output projection
	 void addProjection(unsigned int id) { projection.push_back(id); }
	 void addPredicateFlag(unsigned flag) { predicateFlag.push_back(flag); }
	 void setHasPredicate(bool flag) { hasPredicate = flag; }
	 /// Iterator over the projection
	 typedef std::vector<unsigned int>::const_iterator projection_iterator;
	 /// Iterator over the projection
	 projection_iterator projectionBegin() const { return projection.begin(); }
	 /// Iterator over the projection
	 projection_iterator projectionEnd() const { return projection.end(); }
	 /// project IDs
	 vector<ID>& getProjection() { return projection; }
	 vector<unsigned>& getPredicateFlag() { return predicateFlag; }
	 bool getHasPredicate() { return hasPredicate; }

	bool isPredicateConst();
};

#endif /* TRIPLEBITQUERYGRAPH_H_ */

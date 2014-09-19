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

#include "QuerySemanticAnalysis.h"
#include <algorithm>
#include "SPARQLParser.h"
#include "TripleBitQueryGraph.h"
#include "IRepository.h"
#include <set>

//#define TEST_StrtoID_TIME 1

using namespace std;

QuerySemanticAnalysis::QuerySemanticAnalysis(IRepository &repo) : repo(repo){
	// TODO Auto-generated constructor stub

}

QuerySemanticAnalysis::~QuerySemanticAnalysis() {
	// TODO Auto-generated destructor stub
}

extern bool isUnused(const TripleBitQueryGraph::SubQuery& query,const TripleBitQueryGraph::TripleNode& node,unsigned val);
//---------------------------------------------------------------------------
static bool binds(const SPARQLParser::PatternGroup& group, ID id)
   // Check if a variable is bound in a pattern group
{
   for (std::vector<SPARQLParser::Pattern>::const_iterator iter=group.patterns.begin(),limit=group.patterns.end();iter!=limit;++iter)
      if ((((*iter).subject.type==SPARQLParser::Element::Variable)&&((*iter).subject.id==id))||
          (((*iter).predicate.type==SPARQLParser::Element::Variable)&&((*iter).predicate.id==id))||
          (((*iter).object.type==SPARQLParser::Element::Variable)&&((*iter).object.id==id)))
         return true;
   for (std::vector<SPARQLParser::PatternGroup>::const_iterator iter=group.optional.begin(),limit=group.optional.end();iter!=limit;++iter)
      if (binds(*iter,id))
         return true;
   for (std::vector<std::vector<SPARQLParser::PatternGroup> >::const_iterator iter=group.unions.begin(),limit=group.unions.end();iter!=limit;++iter)
      for (std::vector<SPARQLParser::PatternGroup>::const_iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2)
         if (binds(*iter2,id))
            return true;
   return false;
}

//---------------------------------------------------------------------------
static bool encodeFilter(IRepository &repo,const SPARQLParser::PatternGroup& group,const SPARQLParser::Filter& input,TripleBitQueryGraph::SubQuery& output)
   // Encode an element for the query graph
{
	// Check if the id is bound somewhere
	if (!binds(group,input.id))
		return false;

	// A complex filter? XXX handles only primitive filters
	if ((input.values.size()==1)&&(input.values[0].type==SPARQLParser::Element::Variable)) {
		if (!binds(group,input.id))
			return input.type==SPARQLParser::Filter::Exclude;
		TripleBitQueryGraph::ComplexFilter filter;
		filter.id1=input.id;
		filter.id2=input.values[0].id;
		filter.equal=(input.type==SPARQLParser::Filter::Normal);
		output.complexFilters.push_back(filter);
		return true;
	}

	// Resolve all values
	std::set<unsigned> values;
	for (std::vector<SPARQLParser::Element>::const_iterator iter=input.values.begin(),limit=input.values.end();iter!=limit;++iter) {
		unsigned id;
		if (repo.lookup(iter->value, id))
			values.insert(id);
	}

	// Construct the filter
	TripleBitQueryGraph::Filter filter;
	filter.id=input.id;
	filter.values.clear();
	if (input.type!=SPARQLParser::Filter::Path) {
		for (std::set<unsigned>::const_iterator iter=values.begin(),limit=values.end();iter!=limit;++iter)
			filter.values.push_back(*iter);
		filter.exclude=(input.type==SPARQLParser::Filter::Exclude);
	} else if (values.size()==2) {
		unsigned target,via;
		repo.lookup(input.values[0].value,target);
		repo.lookup(input.values[1].value,via);

		// Explore the path
		std::set<unsigned> explored;
		std::vector<unsigned> toDo;
		toDo.push_back(target);
		while (!toDo.empty()) {
			// Examine the next reachable node
			unsigned current=toDo.front();
			toDo.erase(toDo.begin());
			if (explored.count(current))
				continue;
			explored.insert(current);
			filter.values.push_back(current);

			// Request all other reachable nodes
			ID id;
			if( repo.getSubjectByObjectPredicate(current, via ) == OK ) {
				while (id == repo.next())
					toDo.push_back(id);
			}
			/*
			FactsSegment::Scan scan;
			if (scan.first(db.getFacts(Database::Order_Predicate_Object_Subject),via,current,0)) {
				while ((scan.getValue1()==via)&&(scan.getValue2()==current)) {
					toDo.push_back(scan.getValue3());
					if (!scan.next())
						break;
				}
			}
			*/
		}
	}

	output.filters.push_back(filter);
	return true;
}

//--------------------------------------------------------------------------
bool static encodeTripleNode(IRepository& repo, const SPARQLParser::Pattern& triplePattern, TripleBitQueryGraph::TripleNode &tripleNode)
// Encode a triple pattern for graph. encode subject,predicate,object to ids, also store their types.
{
	//encode subject node
	switch(triplePattern.subject.type) {
	 case SPARQLParser::Element::Variable:
		 tripleNode.subject = triplePattern.subject.id;
		 tripleNode.constSubject = false;
		 break;
	 case SPARQLParser::Element::String:
	 case SPARQLParser::Element::IRI:
		 if(repo.find_soid_by_string(tripleNode.subject,triplePattern.subject.value)){
			 tripleNode.constSubject = true;
			 break;
		 }else return false;
	 default: return false; // Error, this should not happen!
	}

	//encode object node.
	switch(triplePattern.object.type) {
	 case SPARQLParser::Element::Variable:
		 tripleNode.object = triplePattern.object.id;
		 tripleNode.constObject = false;
		 break;
	 case SPARQLParser::Element::String:
	 case SPARQLParser::Element::IRI:
		 if(repo.find_soid_by_string(tripleNode.object,triplePattern.object.value)){
			 tripleNode.constObject = true;
			 break;
		 }else{
			 cout<<"object not found: "<<triplePattern.object.value<<endl;
			 return false;
		 }
	 default: return false; // Error, this should not happen!
	}

	//encode predicate node.
	switch(triplePattern.predicate.type) {
	 case SPARQLParser::Element::Variable:
		 tripleNode.predicate = triplePattern.predicate.id;
		 tripleNode.constPredicate = false;
		 break;
	 case SPARQLParser::Element::String:
	 case SPARQLParser::Element::IRI:
		 if(repo.find_pid_by_string(tripleNode.predicate,triplePattern.predicate.value)){
			 tripleNode.constPredicate = true;
			 break;
		 }else {
			 cout<<"predicate id not found: "<<triplePattern.predicate.value<<endl;
			 return false;
		 }
	 default: return false; // Error, this should not happen!
	}

	return true;
}

bool static collectJoinVariables(TripleBitQueryGraph::SubQuery& query)
   //collect all variables id from triple nodes.
{
	vector<ID>::iterator iter;

	for(unsigned int  index = 0, limit = query.tripleNodes.size(); index < limit ; index ++) {
	      const TripleBitQueryGraph::TripleNode& tpn=query.tripleNodes[index];
		  if (!tpn.constSubject) {
			  if(isUnused(query, tpn, tpn.subject) == false || query.tripleNodes.size() == 1) {
				  iter = find(query.joinVariables.begin(),query.joinVariables.end(),tpn.subject);
				  if(iter == query.joinVariables.end())
					  query.joinVariables.push_back(tpn.subject);
			  }
	      }
	      if (!tpn.constPredicate) {
			  if(isUnused(query, tpn, tpn.predicate) == false || query.tripleNodes.size() == 1) {
				  iter = find(query.joinVariables.begin(),query.joinVariables.end(),tpn.predicate);
				  if(iter == query.joinVariables.end())
					  query.joinVariables.push_back(tpn.predicate);
			  }
	      }
	      if (!tpn.constObject) {
			  if(isUnused(query, tpn, tpn.object) == false || query.tripleNodes.size() == 1) {
				  iter = find(query.joinVariables.begin(),query.joinVariables.end(),tpn.object);
				  if(iter == query.joinVariables.end())
					  query.joinVariables.push_back(tpn.object);
			  }
	      }
	}

	return true;
}

//bool static
//TODO
bool static encodeJoinVariableNodes(TripleBitQueryGraph::SubQuery& query)
	//encode VariablesNodes,the variable nodes include information about the variable node and triple nodes edges
{
	//construct variable nodes ,fill their members.
	std::vector<TripleBitQueryGraph::JoinVariableNodeID>::size_type limit = query.joinVariables.size();
	query.joinVariableNodes.resize(limit);
	//iterate to check a join variable node's edges
	for(unsigned int i = 0;i < limit;i ++) {
		query.joinVariableNodes[i].value = query.joinVariables[i];

		//cout<<"asdfasdf"<<endl;

		//check if has an edge with  triple node  j.
		std::vector<TripleBitQueryGraph::TripleNode>::size_type triplenodes_size = query.tripleNodes.size();
		for(unsigned int j = 0; j < triplenodes_size; ++ j) {
			if(!query.tripleNodes[j].constSubject) {
				if(query.joinVariableNodes[i].value == query.tripleNodes[j].subject){
					//add an edge
					std::pair<TripleBitQueryGraph::TripleNodeID,TripleBitQueryGraph::JoinVariableNode::DimType> edge;
					edge.first = query.tripleNodes[j].tripleNodeID;
					edge.second = TripleBitQueryGraph::JoinVariableNode::SUBJECT;
					query.joinVariableNodes[i].appear_tpnodes.push_back(edge);
				}
			}
			if(!query.tripleNodes[j].constPredicate){
				if(query.joinVariableNodes[i].value == query.tripleNodes[j].predicate){
					//add an edge
					std::pair<TripleBitQueryGraph::TripleNodeID,TripleBitQueryGraph::JoinVariableNode::DimType> edge;
					edge.first = query.tripleNodes[j].tripleNodeID;
					edge.second = TripleBitQueryGraph::JoinVariableNode::PREDICATE;
					query.joinVariableNodes[i].appear_tpnodes.push_back(edge);
				}
			}
			if(!query.tripleNodes[j].constObject){
				if(query.joinVariableNodes[i].value == query.tripleNodes[j].object){
					//add an edge
					std::pair<TripleBitQueryGraph::TripleNodeID,TripleBitQueryGraph::JoinVariableNode::DimType> edge;
					edge.first = query.tripleNodes[j].tripleNodeID;
					edge.second = TripleBitQueryGraph::JoinVariableNode::OBJCET;
					query.joinVariableNodes[i].appear_tpnodes.push_back(edge);
				}
			}

		}//end of for
	}
	return true;
}

static bool encodeJoinVariableEdges(TripleBitQueryGraph::SubQuery& output)
{
	vector<TripleBitQueryGraph::JoinVariableNode>::size_type size = output.joinVariableNodes.size(), i, j;
	vector< pair < TripleBitQueryGraph::TripleNodeID , TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator iterI,iterJ;

	TripleBitQueryGraph::JoinVariableNodesEdge edge;

	for( i= 0; i < size - 1; i++)
	{
		//iterI = output.joinVariableNodes[i].appear_tpnodes.begin();
		for(j = i+1; j < size; j++)
		{
			iterI = output.joinVariableNodes[i].appear_tpnodes.begin();

			for(; iterI != output.joinVariableNodes[i].appear_tpnodes.end();iterI++){
				iterJ = output.joinVariableNodes[j].appear_tpnodes.begin();
				for( ; iterJ != output.joinVariableNodes[j].appear_tpnodes.end(); iterJ++){
					if(iterI->first == iterJ->first){
						edge.from = output.joinVariableNodes[i].value;
						edge.to = output.joinVariableNodes[j].value;
						output.joinVariableEdges.push_back(edge);
					}
				}
			}
		}
	}
	return true;
}

//---------------------------------------------------------------------------
static bool transformSubquery(IRepository& repo,const SPARQLParser::PatternGroup& group,TripleBitQueryGraph::SubQuery& output)
	// Transform a subquery, encoding PatternGroup, fill the subquery,build the JoinVariable Nodes and their edges with triple nodes.
{
	// Encode all triple patterns
	TripleBitQueryGraph::TripleNode tripleNode;
	unsigned int tr_id = 0;

	struct timeval start, end;
    gettimeofday(&start,NULL);

	for (std::vector<SPARQLParser::Pattern>::const_iterator iter = group.patterns.begin(),limit = group.patterns.end();iter != limit;++ iter, ++ tr_id) {
		// Encode the a triple pattern
		//TripleBitQueryGraph::TripleNode tripleNode;
		if(!encodeTripleNode(repo,(*iter),tripleNode)) return false;
		tripleNode.tripleNodeID = tr_id ;
		output.tripleNodes.push_back(tripleNode);
	}
	gettimeofday(&end,NULL);
#ifdef TEST_StrtoID_TIME
    cerr<<"string>>id time elapsed: "<<((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec )  << "us"<<endl;
#endif
	//TODO
	// Encode the filter conditions
	for (vector<SPARQLParser::Filter>::const_iterator iter = group.filters.begin(), limit = group.filters.end(); iter != limit; iter++) {
		if ( !encodeFilter(repo, group, *iter, output) ) {
			return false;
		}
	}

	//Collect join variables
	collectJoinVariables(output);

	//Encode Join Variable Nodes.
	TripleBitQueryGraph::JoinVariableNode joinVariableNode;
	if(!encodeJoinVariableNodes(output)) return false;

	//TODO Encode Join Variables Edges
	encodeJoinVariableEdges(output);
	//not TODO Encode Join Variable and Triple Node Edge,because such edge already exists in JoinVariableNode.
	//also it can be construct ,all depends on your choice.

	//TODO
	// Encode all optional parts

	//TODO
	// Encode all union parts

	return true;
}
//---------------------------------------------------------------------------

bool projectionVariableIsPredicate(TripleBitQueryGraph::SubQuery &query, unsigned int id)
{
        size_t size = query.tripleNodes.size();

        vector<TripleBitQueryGraph::TripleNode>& node = query.tripleNodes;
        for(size_t i = 0; i < size; i++) {
        	if((node[i].constPredicate == false) && (id == node[i].predicate))return true;
        }

        return false;
}

bool QuerySemanticAnalysis::transform(const SPARQLParser& input,TripleBitQueryGraph& output)
// Perform the transformation
{
	output.clear();

	if (!transformSubquery(repo, input.getPatterns(), output.getQuery())) {
	  // A constant could not be resolved. This will produce an empty result
	  output.markAsKnownEmpty();
	  return true;
   }

   // Compute the subquery edges(always the graph pattern ,option pattern,filter pattern join edges)
   output.constructSubqueryEdges();

   // Add the projection entry
   for (SPARQLParser::projection_iterator iter=input.projectionBegin(),limit=input.projectionEnd();iter!=limit;++iter){
      output.addProjection(*iter);
      if(projectionVariableIsPredicate(output.getQuery(),*iter) == true){
    	  output.addPredicateFlag(1);
    	  output.setHasPredicate(true);
      }
      else output.addPredicateFlag(0);
   }
   //output.query.tripleN

   // Set the duplicate handling
   switch (input.getProjectionModifier()) {
      case SPARQLParser::Modifier_None: output.setDuplicateHandling(TripleBitQueryGraph::AllDuplicates); break;
      case SPARQLParser::Modifier_Distinct: output.setDuplicateHandling(TripleBitQueryGraph::NoDuplicates); break;
      case SPARQLParser::Modifier_Reduced: output.setDuplicateHandling(TripleBitQueryGraph::ReducedDuplicates); break;
      case SPARQLParser::Modifier_Count: output.setDuplicateHandling(TripleBitQueryGraph::CountDuplicates); break;
      case SPARQLParser::Modifier_Duplicates: output.setDuplicateHandling(TripleBitQueryGraph::ShowDuplicates); break;
   }

   // Set the limit
   output.setLimit(input.getLimit());
   return true;
}

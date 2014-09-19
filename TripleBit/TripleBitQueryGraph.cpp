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

#include "TripleBitQueryGraph.h"
#include <set>
using namespace std;

bool TripleBitQueryGraph::TripleNode::canJoin(const TripleNode& other) const
   // Is there an implicit join edge to another node?
{
   // Extract variables
   ID v11=0,v12=0,v13=0;
   if (!constSubject) v11=subject+1;
   if (!constPredicate) v12=predicate+1;
   if (!constObject) v13=object+1;
   ID v21=0,v22=0,v23=0;
   if (!other.constSubject) v21=other.subject+1;
   if (!other.constPredicate) v22=other.predicate+1;
   if (!other.constObject) v23=other.object+1;

   // Do they have a variable in common?
   bool canJoin=false;
   if (v11&&v21&&(v11==v21)) canJoin=true;
   if (v11&&v22&&(v11==v22)) canJoin=true;
   if (v11&&v23&&(v11==v23)) canJoin=true;
   if (v12&&v21&&(v12==v21)) canJoin=true;
   if (v12&&v22&&(v12==v22)) canJoin=true;
   if (v12&&v23&&(v12==v23)) canJoin=true;
   if (v13&&v21&&(v13==v21)) canJoin=true;
   if (v13&&v22&&(v13==v22)) canJoin=true;
   if (v13&&v23&&(v13==v23)) canJoin=true;

   return canJoin;
}

//
TripleBitQueryGraph::TripleNodesEdge::TripleNodesEdge(TripleNodeID from, TripleNodeID to, const std::vector<ID>& common)
   : from(from),to(to),common(common)
   // Constructor
{
}

//
TripleBitQueryGraph::TripleNodesEdge::~TripleNodesEdge()
	//Deconstructor
{

}

//TODO
bool TripleBitQueryGraph::JoinVariableNode::hasEdge(const TripleBitQueryGraph::JoinVariableNode& other)const
{
	return false;
}

//
TripleBitQueryGraph::JoinVariableNodeTripleNodeEdge::JoinVariableNodeTripleNodeEdge(JoinVariableNode& from, TripleNode& to,DimType dimType)
	: from(from),to(to),dimType(dimType)
{
}

//
TripleBitQueryGraph::JoinVariableNodeTripleNodeEdge::~JoinVariableNodeTripleNodeEdge()
{
}

//
TripleBitQueryGraph::TripleBitQueryGraph(): duplicateHandling(AllDuplicates),limit(~0u),knownEmptyResult(false),hasPredicate(false)
	//
{
}

//
TripleBitQueryGraph::~TripleBitQueryGraph()
{
}

void TripleBitQueryGraph::clear()
	// clear the QueryGraph
{
	query=SubQuery();
	duplicateHandling=AllDuplicates;
	knownEmptyResult=false;
	hasPredicate = false;
}

void TripleBitQueryGraph::Clear()
{
	query = SubQuery();
	projection.clear();
	predicateFlag.clear();
	hasPredicate = false;
}

//---------------------------------------------------------------------------
static bool intersects(const set<unsigned int>& a,const set<unsigned int>& b,vector<ID>& common)
   // Check if two sets overlap
{
   common.clear();
   set<unsigned int>::const_iterator ia,la,ib,lb;
   if (a.size()<b.size()) {
      if (a.empty())
         return false;
      ia=a.begin(); la=a.end();
      ib=b.lower_bound(*ia); lb=b.end();
   } else {
      if (b.empty())
         return false;
      ib=b.begin(); lb=b.end();
      ia=a.lower_bound(*ib); la=a.end();
   }
   bool result=false;
   while ((ia!=la)&&(ib!=lb)) {
      unsigned va=*ia,vb=*ib;
      if (va<vb) {
         ++ia;
      } else if (va>vb) {
         ++ib;
      } else {
         result=true;
         common.push_back(*ia);
         ++ia; ++ib;
      }
   }
   return result;
}

//---------------------------------------------------------------------------
void TripleBitQueryGraph::constructSubqueryEdges()
   // Construct the edges for a specific subquery
{
	set<unsigned int> bindings;
   //#############################################################
   // Collect all variable bindings
   vector<set<ID> > patternBindings;//
   //TODO optionalBindings,unionBindings

   set<JoinVariableNodeID> TripleNodesVariables;
   //Collect Patterns variable
   patternBindings.resize(query.tripleNodes.size());
   for (unsigned int index=0,limit=patternBindings.size();index<limit;++index) {
      const TripleBitQueryGraph::TripleNode& tpn=query.tripleNodes[index];
      if (!tpn.constSubject) {
         patternBindings[index].insert(tpn.subject);
         bindings.insert(tpn.subject);
         TripleNodesVariables.insert(tpn.subject);
      }
      if (!tpn.constPredicate) {
         patternBindings[index].insert(tpn.predicate);
         bindings.insert(tpn.predicate);
         TripleNodesVariables.insert(tpn.predicate);
      }
      if (!tpn.constObject) {
         patternBindings[index].insert(tpn.object);
         bindings.insert(tpn.object);
         TripleNodesVariables.insert(tpn.object);
      }
   }
   //TODO Collect OptionBindings variable
   //TODO Collect UnionBindings  variable

   // Derive all edges
   query.tripleEdges.clear();
   //query.joinVariableNodes.clear();

   ///triple,TODO option, unions node edges
   vector<ID> common; //common variable
   for (unsigned int index=0,limit=patternBindings.size();index<limit;++index) {
	   //triple patterns edges
      for (unsigned int index2=index+1;index2<limit;index2++)
         if (intersects(patternBindings[index],patternBindings[index2],common)){
        	 query.tripleEdges.push_back(TripleBitQueryGraph::TripleNodesEdge(index,index2,common));
         }
      //TODO option unions edges
   }
}

bool TripleBitQueryGraph::isPredicateConst()
{
        size_t size = query.tripleNodes.size();

        vector<TripleNode>& node = query.tripleNodes;
        for(size_t i = 0; i < size; i++) {
                if(node[i].constPredicate == false)
                        return false;
        }

        return true;
}

#ifndef SPARQLParser_H_
#define SPARQLParser_H_

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

#include <map>
#include <string>
#include <vector>
//---------------------------------------------------------------------------
class SPARQLLexer;
//---------------------------------------------------------------------------
/// A parser for SPARQL input
class SPARQLParser
{
public:
   /// A parsing exception
   struct ParserException {
      /// The message
      std::string message;

      /// Constructor
      ParserException(const std::string& message);
      /// Constructor
      ParserException(const char* message);
      /// Destructor
      ~ParserException();
   };
   /// An element in a graph pattern
   struct Element {
      /// Possible types
      enum Type { Variable, String, IRI };
      /// The type
      Type type;
      /// The string value
      std::string value;
      /// The id for variables
      unsigned id;
   };

   /// A graph pattern
   struct Pattern {
      /// The entires
      Element subject,predicate,object;

      /// Constructor
      Pattern(Element subject,Element predicate,Element object);
      /// Destructor
      ~Pattern();
   };
   /// A filter condition
   struct Filter {
      /// Possible types
      enum Type { Normal, Exclude, Path };

      /// The filtered variable
      unsigned id;
      /// Valid entries
      std::vector<Element> values;
      /// The type
      Type type;
   };
   /// A group of patterns
   struct PatternGroup {
      /// The patterns
      std::vector<Pattern> patterns;
      /// The filter conditions
      std::vector<Filter> filters;
      /// The optional parts
      std::vector<PatternGroup> optional;
      /// The union parts
      std::vector<std::vector<PatternGroup> > unions;
   };
   /// The projection modifier
   enum ProjectionModifier { Modifier_None, Modifier_Distinct, Modifier_Reduced, Modifier_Count, Modifier_Duplicates };

private:
   /// The lexer
   SPARQLLexer& lexer;
   /// The registered prefixes
   std::map<std::string,std::string> prefixes;
   /// The named variables
   std::map<std::string,unsigned> namedVariables;
   /// The total variable count
   unsigned variableCount;

   /// The projection modifier
   ProjectionModifier projectionModifier;
   /// The projection clause
   std::vector<unsigned> projection;
   /// The pattern
   PatternGroup patterns;
   /// The result limit
   unsigned limit;

   /// Lookup or create a named variable
   unsigned nameVariable(const std::string& name);

   /// Parse a filter condition
   void parseFilter(PatternGroup& group,std::map<std::string,unsigned>& localVars);
   /// Parse an entry in a pattern
   Element parsePatternElement(PatternGroup& group,std::map<std::string,unsigned>& localVars);
   /// Parse blank node patterns
   Element parseBlankNode(PatternGroup& group,std::map<std::string,unsigned>& localVars);
   // Parse a graph pattern
   void parseGraphPattern(PatternGroup& group);
   // Parse a group of patterns
   void parseGroupGraphPattern(PatternGroup& group);

   /// Parse the prefix part if any
   void parsePrefix();
   /// Parse the projection
   void parseProjection();
   /// Parse the from part if any
   void parseFrom();
   /// Parse the where part if any
   void parseWhere();
   /// Parse the limit part if any
   void parseLimit();

public:
   /// Constructor
   explicit SPARQLParser(SPARQLLexer& lexer);
   /// Destructor
   ~SPARQLParser();

   /// Parse the input. Throws an exception in the case of an error
   void parse();

   /// Get the patterns
   const PatternGroup& getPatterns() const { return patterns; }

   /// Iterator over the projection clause
   typedef std::vector<unsigned>::const_iterator projection_iterator;
   /// Iterator over the projection
   projection_iterator projectionBegin() const { return projection.begin(); }
   /// Iterator over the projection
   projection_iterator projectionEnd() const { return projection.end(); }

   /// The projection modifier
   ProjectionModifier getProjectionModifier() const { return projectionModifier; }
   /// The size limit
   unsigned int getLimit() const { return limit; }
};
//---------------------------------------------------------------------------
#endif

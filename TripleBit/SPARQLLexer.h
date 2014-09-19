#ifndef SPARQLLexer_H_
#define SPARQLLexer_H_

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

#include <string>
//---------------------------------------------------------------------------
/// A lexer for SPARQL input
class SPARQLLexer
{
   public:
   /// Possible tokens
   enum Token { None, Error, Eof, IRI, String, Variable, Identifier, Colon, Semicolon, Comma, Dot, Star, Underscore, LCurly, RCurly, LParen, RParen, LBracket, RBracket, Anon, Equal, NotEqual };

   private:
   /// The input
   std::string input;
   /// The current position
   std::string::const_iterator pos;
   /// The start of the current token
   std::string::const_iterator tokenStart;
   /// The end of the curent token. Only set if delimiters are stripped
   std::string::const_iterator tokenEnd;
   /// The token put back with unget
   Token putBack;
   /// Was the doken end set?
   bool hasTokenEnd;

   public:
   /// Constructor
   SPARQLLexer(const std::string& input);
   /// Destructor
   ~SPARQLLexer();

   /// Get the next token
   Token getNext();
   /// Get the value of the current token
   std::string getTokenValue() const;
   /// Check if the current token matches a keyword
   bool isKeyword(const char* keyword) const;
   /// Put the last token back
   void unget(Token value) { putBack=value; }
};
//---------------------------------------------------------------------------
#endif

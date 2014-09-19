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

#include "TripleBitBuilder.h"
#include "OSFile.h"
#include <raptor.h>

static TempFile rawFacts("./test");

void rdfParser(void* user_data, const raptor_statement* triple)
{
    TripleBitBuilder *builder = (TripleBitBuilder*)user_data;

    char* predicate = (char*)triple->predicate;
	char* subject = (char*)triple->subject;
	char* object = (char*)triple->object;


	if(strlen(predicate) && strlen(subject) && strlen(object))
		builder-> NTriplesParse(subject, predicate, object, rawFacts);
}

void parserRDFFile(string fileName, raptor_statement_handler handler, void* user_data)
{
        raptor_parser *rdf_parser;
        raptor_uri *uri, *base_uri;
        unsigned char* uri_string;

        raptor_init();
        rdf_parser = raptor_new_parser("rdfxml");
        raptor_set_statement_handler(rdf_parser, user_data, handler);

        uri_string = raptor_uri_filename_to_uri_string(fileName.c_str());
        uri = raptor_new_uri(uri_string);
        base_uri = raptor_uri_copy(uri);
        raptor_parse_file(rdf_parser, uri, base_uri);

        raptor_free_parser(rdf_parser);
        raptor_free_uri(base_uri);
        raptor_free_uri(uri);
        raptor_free_memory(uri_string);

        raptor_finish();
}

char* DATABASE_PATH;
int main(int argc, char* argv[])
{
	if(argc != 3) {
		fprintf(stderr, "Usage: %s <RDF file name> <Database Directory>\n", argv[0]);
		return -1;
	}

	if(OSFile::directoryExists(argv[2]) == false) {
		OSFile::mkdir(argv[2]);
	}

	DATABASE_PATH = argv[2];
	TripleBitBuilder* builder = new TripleBitBuilder(argv[2]);
	parserRDFFile(argv[1], rdfParser, builder);
	
	TempFile facts(argv[1]);
	builder->resolveTriples(rawFacts, facts);
	facts.discard();
	
	builder->endBuild();
 	delete builder;

 	return 0;
}

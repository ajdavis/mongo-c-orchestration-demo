#include <mongoc.h>


const char *USAGE =
   "./mongo-c-orchestration-demo TEST.JSON [CONDUCTION_CONNECTION_STRING]\n"
   "\n"
   "Connect to a running Conduction server and run the tests in TEST.JSON.\n"
   "See http://mongo-conduction.readthedocs.org/\n";


bool
run_command (mongoc_database_t *conduction,
             bson_t *command)
{
   bson_t reply;
   bson_error_t error;
   char *str = NULL;

   str = bson_as_json (command, NULL);
   fprintf (stdout, "%s -->\n\n", str);
   bson_free (str);

   if (!mongoc_database_command_simple (conduction, command, NULL, &reply,
                                        &error)) {
      fprintf (stderr, "Conduction command failure: %s\n\n", error.message);
      goto fail;
   }

   str = bson_as_json (&reply, NULL);
   fprintf (stdout, "\t<-- %s\n\n", str);
   bson_free (str);
   bson_destroy (&reply);

   return true;

fail:
   bson_free (str);
   bson_destroy (&reply);
   return false;
}

bool
json_file_to_bson (const char *json_filename,
                   bson_t     *bson)
{
   FILE *f = fopen (json_filename, "rb");
   size_t length;
   char *buffer = NULL;
   bson_error_t error;

   if (!f) {
      perror (json_filename);
      goto fail;
   }

   fseek (f, 0, SEEK_END);
   length = (size_t)ftell (f);
   fseek (f, 0, SEEK_SET);
   buffer = bson_malloc (length);

   if (!buffer) {
      fprintf (stderr, "could not alloc %zu bytes\n", length);
      goto fail;
   }

   if (length != fread (buffer, 1, length, f)) {
      perror (json_filename);
      goto fail;
   }

   if (!bson_init_from_json (bson, buffer, length, &error)) {
      fprintf (stderr, "%s: %s\n", json_filename, error.message);
      goto fail;
   }

   fclose (f);
   free (buffer);
   return true;

fail:
   fclose (f);
   free (buffer);
   return false;
}

bool
topology_test_print_info (bson_t *test_spec)
{
   const char *str;
   bson_iter_t iter;

   if (!(bson_iter_init_find_case (&iter, test_spec, "description") &&
         BSON_ITER_HOLDS_UTF8 (&iter) &&
         (str = bson_iter_utf8 (&iter, NULL)))) {
      goto fail;
   }

   printf ("description: %s\n", str);

   if (!(bson_iter_init_find_case (&iter, test_spec, "type") &&
         BSON_ITER_HOLDS_UTF8 (&iter) &&
         (str = bson_iter_utf8 (&iter, NULL)))) {
      goto fail;
   }

   printf ("type: %s\n", str);

   return true;
fail:
   return false;
}

bool
topology_test_init_config (mongoc_database_t *conduction,
                           bson_t            *test_spec)
{
   bson_iter_t iter;
   bson_t init_config;
   uint32_t length = 0;
   const uint8_t *document = NULL;
   bson_t command;

   if (!(bson_iter_init_find_case (&iter, test_spec, "initConfig") &&
         BSON_ITER_HOLDS_DOCUMENT (&iter))) {
      fprintf (stderr, "missing initConfig\n");
      goto fail;
   }

   bson_iter_document (&iter, &length, &document);
   if (!bson_init_static (&init_config, document, length)) {
      fprintf (stderr, "couldn't parse initConfig\n");
      goto fail;
   }

   bson_init (&command);
   bson_append_utf8 (&command, "post", -1, "/v1/replica_sets", -1);
   bson_append_document (&command, "body", -1, &init_config);
   if (!run_command (conduction, &command)) {
      goto fail;
   }

   return true;
fail:
   return false;
}

int
main (int   argc,
      char *argv[])
{
   mongoc_client_t *conduction_client;
   mongoc_database_t *conduction;
   const char *json_filename = NULL;
   bson_t test_spec;
   const char *uristr = "mongodb://127.0.0.1/";

   mongoc_init ();

   if (argc < 2) {
      fprintf (stderr, "%s\n", USAGE);
      return EXIT_FAILURE;
   }

   json_filename = argv [1];

   if (argc > 2) {
      uristr = argv [2];
   }

   conduction_client = mongoc_client_new (uristr);

   if (!conduction_client) {
      fprintf (stderr, "Failed to parse URI.\n");
      return EXIT_FAILURE;
   }

   conduction = mongoc_client_get_database (conduction_client, "test");

   bson_init (&test_spec);

   if (!(json_file_to_bson (json_filename, &test_spec)
         && topology_test_print_info (&test_spec)
         && topology_test_init_config (conduction, &test_spec))) {
      return EXIT_FAILURE;
   }

   mongoc_database_destroy (conduction);
   mongoc_client_destroy (conduction_client);

   mongoc_cleanup ();

   return EXIT_SUCCESS;
}

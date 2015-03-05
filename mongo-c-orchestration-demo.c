#include <mongoc.h>


const char *USAGE =
   "./mongo-c-orchestration-demo TEST.JSON [CONDUCTION_CONNECTION_STRING]\n"
   "\n"
   "Connect to a running Conduction server and run the tests in TEST.JSON.\n"
   "See http://mongo-conduction.readthedocs.org/\n";


bool
run_command (mongoc_database_t *conduction,
             bson_t            *command,
             bson_t            *reply)
{
   bson_error_t error;
   char *str = NULL;

   str = bson_as_json (command, NULL);
   printf ("%s -->\n\n", str);
   bson_free (str);
   str = NULL;

   /* TODO: print reply even if error. */
   if (!mongoc_database_command_simple (conduction, command, NULL, reply,
                                        &error)) {
      fprintf (stderr, "Conduction command failure: %s\n\n", error.message);
      fflush (stderr);
      goto fail;
   }

   str = bson_as_json (reply, NULL);
   printf ("\t<-- %s\n\n", str);
   fflush (stdout);
   bson_free (str);

   return true;

fail:
   bson_free (str);
   return false;
}

bool
json_command (mongoc_database_t *database,
              const char        *json)
{
   bson_t command;
   bson_t reply;
   bson_error_t error;
   char *str;

   if (!bson_init_from_json (&command, json, -1, &error)) {
      fprintf (stderr, "JSON parse error: %s\n", error.message);
      return false;
   }

   str = bson_as_json (&command, NULL);
   printf ("%s -->\n\n", str);

   /* TODO: factor with run_command */
   bson_init (&reply);

   if (!mongoc_database_command_simple (database, &command, NULL, &reply,
                                        &error)) {
      fprintf (stderr, "Command failure: %s\n\n", error.message);
      return EXIT_FAILURE;
   }

   bson_free (str);
   str = bson_as_json (&reply, NULL);
   printf ("\t<-- %s\n\n", str);
   fflush (stdout);
   bson_free (str);
   bson_destroy (&command);
   bson_destroy (&reply);
   return true;
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
                           bson_t            *test_spec,
                           bson_t            *init_config_reply)
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
   /* TODO: choose /servers, replica_sets, or sharded_clusters */
   bson_append_utf8 (&command, "post", -1, "/v1/sharded_clusters", -1);
   bson_append_document (&command, "body", -1, &init_config);

   if (!run_command (conduction, &command, init_config_reply)) {
      goto fail;
   }

   return true;
fail:
   return false;
}

bool
topology_test_destroy_config (mongoc_database_t *conduction,
                              bson_t            *test_spec)
{
   bson_iter_t iter;
   bson_iter_t init_config_iter;
   /* TODO: choose /servers, /replica_sets, or /sharded_clusters */
   const char *base_path = "/v1/sharded_clusters/";
   const char *config_id;
   size_t path_length;
   char *path = NULL;
   bson_t command;
   bson_t reply;

   /* TODO: factor with topology_test_init_config, or use find_dotted. */
   if (!(bson_iter_init_find_case (&iter, test_spec, "initConfig") &&
         BSON_ITER_HOLDS_DOCUMENT (&iter) &&
         bson_iter_recurse (&iter, &init_config_iter))) {
      fprintf (stderr, "missing initConfig\n");
      goto fail;
   }

   if (!(bson_iter_find_case (&init_config_iter, "id") &&
         (config_id = bson_iter_utf8 (&init_config_iter, NULL)))) {
      fprintf (stderr, "couldn't parse config id from initConfig\n");
      goto fail;
   }

   path_length = strlen (base_path) + strlen (config_id) + 1;
   path = malloc (sizeof (char) * path_length);

   if (!path) {
      fprintf (stderr, "Couldn't alloc string\n");
      goto fail;
   }

   /* Make a path like "/v1/replica_sets/my_id". */
   if ((bson_snprintf (path, path_length, "%s%s", base_path,
                       config_id) + 1) < path_length) {
      fprintf (stderr, "internal error formatting config URL\n");
      goto fail;
   }

   bson_init (&command);
   bson_append_utf8 (&command, "delete", -1, path, -1);

   if (!run_command (conduction, &command, &reply)) {
      goto fail;
   }

   bson_destroy (&command);
   free (path);
   return true;
fail:
   bson_destroy (&command);
   free (path);
   return false;
}

const char *
topology_test_get_mongodb_uri (bson_t *config_reply)
{
   bson_iter_t iter;

   /* TODO: print errors */
   if (!bson_iter_init_find_case (&iter, config_reply, "mongodb_uri")) {
      return NULL;
   }

   if (BSON_ITER_HOLDS_UTF8 (&iter)) {
      return bson_iter_utf8 (&iter, NULL);
   }

   return NULL;
}

int
main (int   argc,
      char *argv[])
{
   mongoc_client_t *conduction_client = NULL;
   mongoc_database_t *conduction = NULL;
   const char *json_filename = NULL;
   bson_t test_spec;
   const char *conduction_uri = "mongodb://127.0.0.1/";
   bson_t init_config_reply;
   const char *mongodb_uri = NULL;
   mongoc_client_t *client = NULL;
   mongoc_collection_t *collection = NULL;

   mongoc_init ();

   if (argc < 2) {
      fprintf (stderr, "%s\n", USAGE);
      return EXIT_FAILURE;
   }

   json_filename = argv [1];

   if (argc > 2) {
      conduction_uri = argv [2];
   }

   conduction_client = mongoc_client_new (conduction_uri);

   if (!conduction_client) {
      fprintf (stderr, "Failed to parse URI.\n");
      return EXIT_FAILURE;
   }

   conduction = mongoc_client_get_database (conduction_client, "test");

   bson_init (&test_spec);
   bson_init (&init_config_reply);

   if (!(json_file_to_bson (json_filename, &test_spec)
         && topology_test_print_info (&test_spec)
         && topology_test_init_config (conduction, &test_spec,
                                       &init_config_reply))) {
      return EXIT_FAILURE;
   }

   if (!(mongodb_uri = topology_test_get_mongodb_uri (&init_config_reply))) {
      fprintf (stderr, "Couldn't find mongodb_uri in Conduction reply\n");
      return EXIT_FAILURE;
   }

   printf("mongodb_uri: %s\n", mongodb_uri);
   client = mongoc_client_new (mongodb_uri);

   /* TODO: read and execute phases */
   json_command (conduction,
                 "{ method: \"POST\", "
                 "uri: \"/servers/mongosA\", "
                 "payload: { action: \"stop\" }");

   collection = mongoc_client_get_collection (client, "test", "test");
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);

   if (!topology_test_destroy_config (conduction, &test_spec)) {
      return EXIT_FAILURE;
   }

   mongoc_database_destroy (conduction);
   mongoc_client_destroy (conduction_client);

   mongoc_cleanup ();

   return EXIT_SUCCESS;
}

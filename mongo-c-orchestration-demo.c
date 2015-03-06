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
   bool result = true;

   str = bson_as_json (command, NULL);
   MONGOC_INFO ("%s -->", str);
   bson_free (str);
   if (!mongoc_database_command_simple (conduction, command, NULL, reply,
                                        &error)) {
      MONGOC_ERROR ("Conduction command failure: %s", error.message);
      result = false;
   }

   str = bson_as_json (reply, NULL);
   MONGOC_INFO ("\t<-- %s", str);
   bson_free (str);

   return result;
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
      MONGOC_ERROR ("JSON parse error: %s", error.message);
      return false;
   }

   str = bson_as_json (&command, NULL);
   MONGOC_INFO ("%s -->", str);

   /* TODO: factor with run_command */
   bson_init (&reply);

   if (!mongoc_database_command_simple (database, &command, NULL, &reply,
                                        &error)) {
      MONGOC_ERROR ("Command failure: %s", error.message);
      return EXIT_FAILURE;
   }

   bson_free (str);
   str = bson_as_json (&reply, NULL);
   MONGOC_INFO ("\t<-- %s", str);
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
      MONGOC_ERROR ("couldn't alloc %zu bytes", length);
      goto fail;
   }

   if (length != fread (buffer, 1, length, f)) {
      perror (json_filename);
      goto fail;
   }

   if (!bson_init_from_json (bson, buffer, length, &error)) {
      MONGOC_ERROR ("%s: %s", json_filename, error.message);
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

   MONGOC_INFO ("description: %s", str);

   if (!(bson_iter_init_find_case (&iter, test_spec, "type") &&
         BSON_ITER_HOLDS_UTF8 (&iter) &&
         (str = bson_iter_utf8 (&iter, NULL)))) {
      goto fail;
   }

   MONGOC_INFO ("type: %s", str);

   return true;
fail:
   return false;
}

/*
 * Return a string like "/v1/replica_sets" or NULL.
 *
 * Caller must free returned string.
 */

char *
deployment_uri (const bson_t *test_spec,
                bool          include_deployment_id)
{
   const char *base_path = "/v1";
   bson_iter_t iter;
   const char *type_str;
   bson_iter_t deployment_iter;
   const char *sep = "";
   const char *deployment_id = "";
   size_t path_length;
   char *path = NULL;

   if (!(bson_iter_init_find_case (&iter, test_spec, "type") &&
         BSON_ITER_HOLDS_UTF8 (&iter) &&
         (type_str = bson_iter_utf8 (&iter, NULL)))) {
      MONGOC_ERROR ("missing \"type\"");
      goto fail;
   }

   if (!strcmp (type_str, "Standalone")) {
      type_str = "servers";
   } else if (!strcmp (type_str, "Sharded")) {
      type_str = "sharded_clusters";
   } else if (!strcmp (type_str, "Standalone")) {
      type_str = "replica_sets";
   } else {
      MONGOC_ERROR ("unrecognized type string: %s", type_str);
      goto fail;
   }

   if (include_deployment_id) {
      bson_iter_init (&iter, test_spec);

      if (!(bson_iter_find_descendant (&iter, "initConfig.id",
                                       &deployment_iter) &&
            BSON_ITER_HOLDS_UTF8 (&deployment_iter) &&
            (deployment_id = bson_iter_utf8 (&deployment_iter, NULL)))) {
         MONGOC_ERROR ("missing initConfig.id");
         goto fail;
      }

      sep = "/";
   }

   /* + 2: two slashes and a NULL terminator. */
   path_length = strlen (base_path) + strlen (type_str) +
                 +strlen (sep) + strlen (deployment_id) + 2;

   if (!(path = malloc (sizeof (char) * path_length))) {
      MONGOC_ERROR ("couldn't alloc string");
      goto fail;
   }

   /* Make a path like "/v1/replica_sets/my_id". */
   if ((bson_snprintf (path, path_length, "%s/%s%s%s", base_path, type_str,
                       sep, deployment_id) + 1) < path_length) {
      MONGOC_ERROR ("internal error formatting server type URL");
      goto fail;
   }

   return path;
fail:
   free (path);
   return NULL;
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
   char *path = NULL;

   if (!(path = deployment_uri (test_spec, false))) {
      goto fail;
   }

   if (!(bson_iter_init_find_case (&iter, test_spec, "initConfig") &&
         BSON_ITER_HOLDS_DOCUMENT (&iter))) {
      MONGOC_ERROR ("missing initConfig");
      goto fail;
   }

   bson_iter_document (&iter, &length, &document);

   if (!bson_init_static (&init_config, document, length)) {
      MONGOC_ERROR ("couldn't parse initConfig");
      goto fail;
   }

   bson_init (&command);
   bson_append_utf8 (&command, "post", -1, path, -1);
   bson_append_document (&command, "body", -1, &init_config);

   if (!run_command (conduction, &command, init_config_reply)) {
      goto fail;
   }

   free (path);
   return true;
fail:
   free (path);
   return false;
}

bool
topology_test_destroy_config (mongoc_database_t *conduction,
                              bson_t            *test_spec)
{
   bson_t command;
   bson_t reply;
   char *path = NULL;

   if (!(path = deployment_uri (test_spec, true))) {
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

   if (!bson_iter_init_find_case (&iter, config_reply, "mongodb_uri")) {
      MONGOC_ERROR("no mongodb uri in response");
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
      MONGOC_ERROR ("Failed to parse URI.");
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
      MONGOC_ERROR ("Couldn't find mongodb_uri in Conduction reply");
      return EXIT_FAILURE;
   }

   MONGOC_INFO ("mongodb_uri: %s", mongodb_uri);
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

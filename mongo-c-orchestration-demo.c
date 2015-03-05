/* ./mongo-c-orchestration-demo [CONNECTION_STRING] */

#include <mongoc.h>
#include <stdio.h>
#include <stdlib.h>


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
   fprintf (stdout, "%s -->\n\n", str);

   bson_init (&reply);

   if (!mongoc_database_command_simple (database, &command, NULL, &reply,
                                        &error)) {
      fprintf (stderr, "Command failure: %s\n\n", error.message);
      return EXIT_FAILURE;
   }

   bson_free (str);
   str = bson_as_json (&reply, NULL);
   fprintf (stdout, "\t<-- %s\n\n", str);
   bson_free (str);
   bson_destroy (&command);
   bson_destroy (&reply);
   return true;
}

int
main (int   argc,
      char *argv[])
{
   mongoc_client_t *client;
   mongoc_database_t *database;
   const char *uristr = "mongodb://127.0.0.1/";

   mongoc_init ();

   if (argc > 1) {
      uristr = argv [1];
   }

   client = mongoc_client_new (uristr);

   if (!client) {
      fprintf (stderr, "Failed to parse URI.\n");
      return EXIT_FAILURE;
   }

   database = mongoc_client_get_database (client, "test");

   /* Debugging aid: in case a server wasn't cleaned up from a previous run. */
   if (!json_command (database, "{\"delete\": \"/v1/servers/my_id\"}")) {
      return EXIT_FAILURE;
   }

   if (!json_command (database,
                      "{\"post\": \"/servers\", "
                      "\"body\": {"
                      "\"name\": \"mongod\", "
                      "\"preset\": \"basic.json\", "
                      "\"id\": \"my_id\"}}"
                      )) {
      return EXIT_FAILURE;
   }

   if (!json_command (database, "{\"delete\": \"/v1/servers/my_id\"}")) {
      return EXIT_FAILURE;
   }

   mongoc_database_destroy (database);
   mongoc_client_destroy (client);

   mongoc_cleanup ();

   return EXIT_SUCCESS;
}

package org.finos.legend.pylegend;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

@Path("/data")
public class DataShow
{
    @GET
    @Path("/showtables")
    @Produces(MediaType.APPLICATION_JSON)
    public Response showAllTables()
    {
        List<String> tablePaths = new ArrayList<>();

        for (Map.Entry<String, Map<String, Map<String, Map<String, ServerState.TableMetadata>>>> warehouseEntry : ServerState.data.entrySet())
        {
            String warehouse = warehouseEntry.getKey();
            for (Map.Entry<String, Map<String, Map<String, ServerState.TableMetadata>>> dbEntry : warehouseEntry.getValue().entrySet())
            {
                String db = dbEntry.getKey();
                for (Map.Entry<String, Map<String, ServerState.TableMetadata>> schemaEntry : dbEntry.getValue().entrySet())
                {
                    String schema = schemaEntry.getKey();
                    for (String table : schemaEntry.getValue().keySet())
                    {
                        String fullPath = String.join("::", warehouse, db, schema, table);
                        tablePaths.add(fullPath);
                    }
                }
            }
        }
        Map<String, Object> response = new HashMap<>();
        response.put("tables", tablePaths);
        response.put("count", tablePaths.size());
        return Response.ok(response).build();
    }
}

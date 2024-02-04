package bdd;

import com.mongodb.client.*;
import com.mongodb.MongoCredential;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.IndexOptions;
import org.json.JSONArray;

public class AgentImmo { 
   private MongoDatabase database;
   private String dbName="Agence_immobilière";
   private String hostName="localhost";
   private int port=27017;
   private String userName="test";
   private String passWord="test";
   private String agentImmoCollectionName="EstateAgent";
   
   public static void main( String args[] ) {  
	    try{
	    	AgentImmo AgentImmo= new AgentImmo();
	    	AgentImmo.dropCollectionAgentImmo(AgentImmo.agentImmoCollectionName);
	    	loadJsonToMongoDB("./data/"+AgentImmo.agentImmoCollectionName+".json", AgentImmo.dbName, AgentImmo.agentImmoCollectionName);
	    	AgentImmo.createCollectionAgentImmo(AgentImmo.agentImmoCollectionName);
	    	AgentImmo.testInsertOneAgentImmo();
	    	AgentImmo.getAgentsSortedBy(AgentImmo.agentImmoCollectionName, "first name");
	    	AgentImmo.deleteAgents(AgentImmo.agentImmoCollectionName, new Document("_id", "12000"));
	    	AgentImmo.getAgentImmo(AgentImmo.agentImmoCollectionName, 
				new Document(), 
				new Document(),
				new Document()
			);
		}catch(Exception e){
			e.printStackTrace();
		}	
	   } 
	   
	   /**
		FV1 : Constructeur AgentImmo.

	   */
	   
   public AgentImmo(){
		// Creating a Mongo client
		
		MongoClient mongoClient = MongoClients.create("mongodb://" + hostName + ":" + port);; 

		// Creating Credentials 
		MongoCredential credential; 
		credential = MongoCredential.createCredential(userName, dbName, 
		 passWord.toCharArray()); 
		System.out.println("Connected to the database successfully"); 	  
		System.out.println("Credentials ::"+ credential);  
		// Accessing the database 
		database = mongoClient.getDatabase(dbName); 

   }
   /**
	FV2 : Cette fonction permet de creer une collection
	de nom AgentImmo.
   */
   public void createCollectionAgentImmo(String nomCollection){
		//Creating a collection 
		database.createCollection(nomCollection); 
		System.out.println("Collection Agents Immobiliers created successfully"); 

   }
   
   
   /**
	FV3 : Cette fonction permet de supprimer une collection
	de nom nomCollection.
   */
   
   public void dropCollectionAgentImmo(String nomCollection){
		//Drop a collection 
		MongoCollection<Document> colAgentImmo=null; 
		

		colAgentImmo=database.getCollection(nomCollection);
		System.out.println("!!!! Collection Vol : "+colAgentImmo);
		// Visiblement jamais !!!
		if (colAgentImmo==null)
			System.out.println("Collection inexistante");
		else {
			colAgentImmo.drop();	
			System.out.println("Collection Agents Immobiliers removed successfully !!!"); 
	  
		}
   }

   /**
	FV4 : Cette fonction permet d'inserer un vol dans une collection.
   */
   
   public void insertOneAgentImmo(String nomCollection, Document agentImmo){
		//Drop a collection 
		MongoCollection<Document> colAgentImmo=database.getCollection(nomCollection);
		colAgentImmo.insertOne(agentImmo); 
		System.out.println("Document inserted successfully");     
   }

   /**
	FV5 : Cette fonction permet de tester la methode insertOneVol.
   */

   public void testInsertOneAgentImmo(){
		Document agentImmo =  new Document("_id", "121")
      .append("villeDepart","Paris")
      .append("villeArrivee", "Nantes")
      .append("heureDepart", "13:15")
      .append("heureArrivee", "14:45")
      .append("dateVol", "14/12/2019")
      .append("appreciations", 
         Arrays.asList(
			new Document("idClient", "07")
            .append("notes", Arrays.asList(
				   new Document("apid", "071")
					  .append("critereANoter", "SiteWeb")
					  .append("note", "BIEN"),
					  
					new Document("apid", "072")
					  .append("critereANoter", "Prix")
					  .append("note", "BIEN"),

					new Document("apid", "073")
					  .append("critereANoter", "Nourritureabord")
					  .append("note", "BIEN"),
				 
					new Document("apid", "074")
					  .append("critereANoter", "Qualitesiege")
					  .append("note", "BIEN"),

					new Document("apid", "075")
					  .append("critereANoter", "Accueilguichet")
					  .append("note", "BIEN"),
					  
					new Document("apid", "076")
					  .append("critereANoter", "Accueilabord")
					  .append("note", "EXCELLENT")
					)
				)
			)
		);
      
		this.insertOneAgentImmo(this.agentImmoCollectionName, agentImmo);
		System.out.println("Document inserted successfully");     
   }

   /**
	FV6 : Cette fonction permet d'inserer plusieurs Vols dans une collection
   */

   public void insertManyAgentImmo(String nomCollection, List<Document> colAgentImmos){
		//Drop a collection 
		MongoCollection<Document> colAgentImmo=database.getCollection(nomCollection);
		colAgentImmo.insertMany(colAgentImmos); 
		System.out.println("Many Documents inserted successfully");     
   }
   
   /**
	FD10 : Cette fonction permet de modifier des agents dans une collection.
	Le parametre whereQuery : permet de passer des conditions de recherche
	Le parametre updateExpressions : permet d'indiquer les champs a modifier
	Le parametre UpdateOptions : permet d'indiquer les options de mise a jour :
		.upSert : insere si le document n'existe pas
   */
   public void updateAgentImmo(String nomCollection, 
	Document whereQuery, 
	Document updateExpressions,
	UpdateOptions updateOptions
	){
		//Drop a collection 
		System.out.println("\n\n\n*********** dans updateAddresses *****************");   

		MongoCollection<Document> colAgentImmo=database.getCollection(nomCollection);
		UpdateResult updateResult = colAgentImmo.updateMany(whereQuery, updateExpressions);
		
		System.out.println("\nResultat update : "
		+"getUpdate id: "+updateResult
		+" getMatchedCount : "+updateResult.getMatchedCount() 
		+" getModifiedCount : "+updateResult.getModifiedCount()
		);
   }
   
   public void deleteAgents(String nomCollection, Document filters){

       System.out.println("\n\n\n*********** dans deleteAgents *****************");
       FindIterable<Document> listAgent;
       Iterator it;
       MongoCollection<Document> colAgents=database.getCollection(nomCollection);

       listAgent=colAgents.find(filters).sort(new Document("_id", 1));
       it = listAgent.iterator();// Getting the iterator
       

       colAgents.deleteMany(filters);
       listAgent=colAgents.find(filters).sort(new Document("_id", 1));
       it = listAgent.iterator();
   }
   
   public void getAgentImmo(String nomCollection, 
			Document whereQuery, 
			Document projectionFields,
			Document sortFields){
				
				System.out.println("\n\n\n*********** dans getAgentImmo*****************");   

				MongoCollection<Document> colAgentImmo=database.getCollection(nomCollection);

				FindIterable<Document> listAgentImmo=colAgentImmo.find(whereQuery).sort(sortFields).projection(projectionFields);

				// Getting the iterator 
				Iterator it = listAgentImmo.iterator();
				while(it.hasNext()) {
						System.out.println(it.next());
				}		
		   } 
   
   // creation d'index secondaire 
   
   public void createIndex(String nomCollection, String champs) {
	   
        MongoCollection<Document> colAgentImmo = database.getCollection(nomCollection);

        
        colAgentImmo.createIndex(Indexes.ascending(champs), new IndexOptions().unique(true));
   }

   // Méthodes applicatives de consultation

   // Exemple de jointure pour récupérer les propriétés associées à un agent immobilier
   
   public AggregateIterable<Document> getPropertiesForAgent(ObjectId agentId,String colAgent,String colTrans,String colProp) {
       MongoCollection<Document> agentsCollection = database.getCollection(colAgent);
       MongoCollection<Document> transactionsCollection = database.getCollection(colTrans);
       MongoCollection<Document> propertiesCollection = database.getCollection(colProp);

       AggregateIterable<Document> result = agentsCollection.aggregate(
               Arrays.asList(
                       Aggregates.match(Filters.eq("_id", agentId)),
                       Aggregates.lookup("transactions", "_id", "realEstateAgentId", "transactions"),
                       Aggregates.unwind("$transactions"),
                       Aggregates.lookup("properties", "transactions.propertyId", "_id", "properties"),
                       Aggregates.unwind("$properties"),
                       Aggregates.group("$properties._id", Accumulators.first("property", "$properties"))
               )
       );

       return result;
   }

   // Méthode pour trier les agents immobiliers par un champs precis
   
   public List<Document> getAgentsSortedBy(String nomCollection, String champs) {
       MongoCollection<Document> collection = database.getCollection(nomCollection);

       return collection.find()
               .sort(Sorts.ascending(champs))
               .into(new ArrayList<Document>());
   }

   /*
   Cette fonction permet de se connecter sur une base de données MongoDB et d'insérer des objets JSON
   en les convertissant en documents
    */
   public static void loadJsonToMongoDB(String filePath, String databaseName, String collectionName) throws IOException {
       try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
           MongoDatabase database = mongoClient.getDatabase(databaseName);
           MongoCollection<Document> collection = database.getCollection(collectionName);

           //On vérifie si la collection existe ou non
           MongoIterable<String> collectionNames = database.listCollectionNames();
           boolean collectionExists = false;
           for (String name : collectionNames) {
               if (name.equals(collectionName)) {
                   collectionExists = true;
                   break;
               }
           }
           //Si la collection n'existe pas on la crée
           if(!collectionExists)
               database.createCollection(collectionName);

           // Lecture du contenu du fichier JSON
           String jsonString = new String(Files.readAllBytes(Paths.get(filePath)));

           // Conversion de la chaîne JSON en un tableau JSON
           JSONArray jsonArray = new JSONArray(jsonString);

           for (int i = 0; i < jsonArray.length(); i++) {
               // On crée un document par objet JSON
               Document document = Document.parse(jsonArray.getJSONObject(i).toString());

               // On insère le document dans la collection
               collection.insertOne(document);
           }

           System.out.println("Documents insérés avec succès dans la collection '" + collectionName + "'");
       }
   }
	   


}
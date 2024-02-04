package bdd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONArray;

import com.mongodb.MongoCredential;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

public class Client {

   private MongoDatabase database;
   private String dbName="Agence_immobilière";
   private String hostName="localhost";
   private int port=27017;
   private String userName="yasmine";
   private String passWord="Yasmine7621";
   private String clientCollectionName="Client";
   
   public static void main( String args[] ) {  
	    try{
	    	Client Client= new Client();
	    	Client.dropCollectionClient(Client.clientCollectionName);
	    	loadJsonToMongoDB("./data/"+Client.clientCollectionName+".json", Client.dbName, Client.clientCollectionName);
	    	Client.createCollectionClient(Client.clientCollectionName);
	    	Client.testInsertOneClient();
	    	Client.getClientsSortedBy("first name");
	    	Client.deleteClients(Client.clientCollectionName, new Document("_id", "12000"));
	    	Client.getClient(Client.clientCollectionName, 
				new Document(), 
				new Document(),
				new Document()
			);
		}catch(Exception e){
			e.printStackTrace();
		}	
	   } 
	   
	   /**
		FV1 : Constructeur Client.

	   */
	   
   public Client(){
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
   public void createCollectionClient(String nomCollection){
		//Creating a collection 
		database.createCollection(nomCollection); 
		System.out.println("Collection Custumers created successfully"); 

   }
   
   
   /**
	FV3 : Cette fonction permet de supprimer une collection
	de nom nomCollection.
   */
   
   public void dropCollectionClient(String nomCollection){
		//Drop a collection 
		MongoCollection<Document> colClient=null; 
		

		colClient=database.getCollection(nomCollection);
		System.out.println("!!!! Collection Vol : "+colClient);
		// Visiblement jamais !!!
		if (colClient==null)
			System.out.println("Collection inexistante");
		else {
			colClient.drop();	
			System.out.println("Collection Custumers removed successfully !!!"); 
	  
		}
   }

   /**
	FV4 : Cette fonction permet d'inserer un vol dans une collection.
   */
   
   public void insertOneClient(String nomCollection, Document agentImmo){
		//Drop a collection 
		MongoCollection<Document> colClient=database.getCollection(nomCollection);
		colClient.insertOne(agentImmo); 
		System.out.println("Document inserted successfully");     
   }

   /**
	FV5 : Cette fonction permet de tester la methode insertOneVol.
   */

   public void testInsertOneClient(){
		Document client =  new Document("_id", "12000")
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
      
		this.insertOneClient(this.clientCollectionName, client);
		System.out.println("Document inserted successfully");     
   }

   /**
	FV6 : Cette fonction permet d'inserer plusieurs Vols dans une collection
   */

   public void insertManyClient(String nomCollection, List<Document> colClient){
		//Drop a collection 
		MongoCollection<Document> colClients=database.getCollection(nomCollection);
		colClients.insertMany(colClient); 
		System.out.println("Many Documents inserted successfully");     
   }
   
   /**
	FD10 : Cette fonction permet de modifier des Client dans une collection.
	Le parametre whereQuery : permet de passer des conditions de recherche
	Le parametre updateExpressions : permet d'indiquer les champs a modifier
	Le parametre UpdateOptions : permet d'indiquer les options de mise a jour :
		.upSert : insere si le document n'existe pas
   */
   public void updateClient(String nomCollection, 
	Document whereQuery, 
	Document updateExpressions,
	UpdateOptions updateOptions
	){
		//Drop a collection 
		System.out.println("\n\n\n*********** dans updateAddresses *****************");   

		MongoCollection<Document> colClient=database.getCollection(nomCollection);
		UpdateResult updateResult = colClient.updateMany(whereQuery, updateExpressions);
		
		System.out.println("\nResultat update : "
		+"getUpdate id: "+updateResult
		+" getMatchedCount : "+updateResult.getMatchedCount() 
		+" getModifiedCount : "+updateResult.getModifiedCount()
		);
   }
   public void deleteClients(String nomCollection, Document filters){

       System.out.println("\n\n\n*********** dans deleteAgents *****************");
       FindIterable<Document> listClient;
       Iterator it;
       MongoCollection<Document> colClients=database.getCollection(nomCollection);

       listClient=colClients.find(filters).sort(new Document("_id", 1));
       it = listClient.iterator();// Getting the iterator
       

       colClients.deleteMany(filters);
       listClient=colClients.find(filters).sort(new Document("_id", 1));
       it = listClient.iterator();
   }
   
   public void getClient(String nomCollection, 
			Document whereQuery, 
			Document projectionFields,
			Document sortFields){
				
				System.out.println("\n\n\n*********** dans getAgentImmo*****************");   

				MongoCollection<Document> colClient=database.getCollection(nomCollection);

				FindIterable<Document> listAgentImmo=colClient.find(whereQuery).sort(sortFields).projection(projectionFields);

				// Getting the iterator 
				Iterator it = listAgentImmo.iterator();
				while(it.hasNext()) {
						System.out.println(it.next());
				}		
		   } 
   
   // creation d'index secondaire 
   
   public void createIndex(String nomCollection, String champs) {
	   
        MongoCollection<Document> colClient = database.getCollection(nomCollection);

        
        colClient.createIndex(Indexes.ascending(champs), new IndexOptions().unique(true));
   }

   // Méthodes applicatives de consultation

  

   // Méthode pour trier les Client par un champs precis
   
   public List<Document> getClientsSortedBy(String champs) {
       MongoCollection<Document> collection = database.getCollection(clientCollectionName);

       return collection.find()
               .sort(Sorts.ascending(champs))
               .into(new ArrayList<Document>());
   }
// Méthode pour effectuer une agrégation du nombre total de transactions associées à ce client
   
   public AggregateIterable<Document> getTotalTransactions(String colTransaction, String idClient) {
       MongoCollection<Document> clientsCollection = database.getCollection(clientCollectionName);
       MongoCollection<Document> transactionsCollection = database.getCollection(colTransaction);

       AggregateIterable<Document> result = clientsCollection.aggregate(
               Arrays.asList(
                       Aggregates.match(Filters.eq("_id",idClient )),
                       Aggregates.lookup("transactions", "_id", "clientId", "transactions"),
                       Aggregates.unwind("$transactions"),
                       Aggregates.group("$_id", Accumulators.sum("totalTransactions", 1)),
                       Aggregates.project(Projections.fields(
                               Projections.excludeId(),
                               Projections.include("totalTransactions")
                       ))
               )
       );

       return result;
   }

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

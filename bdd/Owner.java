package bdd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
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

public class Owner {
   private MongoDatabase database;
   private String dbName="Agence_immobilière";
   private String hostName="localhost";
   private int port=27017;
   private String userName="yasmine";
   private String passWord="Yasmine7621";
   private String ownerCollectionName="Owner";
   
   public static void main( String args[] ) {  
	    try{
	    	Owner Owner= new Owner();
	    	Owner.dropCollectionOwner(Owner.ownerCollectionName);
	    	loadJsonToMongoDB("./data/"+Owner.ownerCollectionName+".json", Owner.dbName, Owner.ownerCollectionName);
	    	Owner.createCollectionOwner(Owner.ownerCollectionName);
	    	Owner.testInsertOneOwner();
	    	Owner.getOwnersSortedBy("name");
	    	Owner.deleteOwners(Owner.ownerCollectionName, new Document("_id", "12000"));
	    	Owner.getOwner(Owner.ownerCollectionName, 
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
	   
   public Owner(){
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
   public void createCollectionOwner(String nomCollection){
		//Creating a collection 
		database.createCollection(nomCollection); 
		System.out.println("Collection Owners created successfully"); 

   }
   
   
   /**
	FV3 : Cette fonction permet de supprimer une collection
	de nom nomCollection.
   */
   
   public void dropCollectionOwner(String nomCollection){
		//Drop a collection 
		MongoCollection<Document> colOwner=null; 
		

		colOwner=database.getCollection(nomCollection);
		System.out.println("!!!! Collection Owner : "+colOwner);
		// Visiblement jamais !!!
		if (colOwner==null)
			System.out.println("Collection inexistante");
		else {
			colOwner.drop();	
			System.out.println("Collection Owner removed successfully !!!"); 
	  
		}
   }

   /**
	FV4 : Cette fonction permet d'inserer un vol dans une collection.
   */
   
   public void insertOneOwner(String nomCollection, Document owner){
		//Drop a collection 
		MongoCollection<Document> colOwner=database.getCollection(nomCollection);
		colOwner.insertOne(owner); 
		System.out.println("Document inserted successfully");     
   }

   /**
	FV5 : Cette fonction permet de tester la methode insertOneVol.
   */

   public void testInsertOneOwner(){
		Document owner =  new Document("_id", "12000")
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
      
		this.insertOneOwner(this.ownerCollectionName, owner);
		System.out.println("Document inserted successfully");     
   }

   /**
	FV6 : Cette fonction permet d'inserer plusieurs Vols dans une collection
   */

   public void insertManyOwner(String nomCollection, List<Document> colClient){
		//Drop a collection 
		MongoCollection<Document> colOwner=database.getCollection(nomCollection);
		colOwner.insertMany(colClient); 
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

		MongoCollection<Document> colOwner=database.getCollection(nomCollection);
		UpdateResult updateResult = colOwner.updateMany(whereQuery, updateExpressions);
		
		System.out.println("\nResultat update : "
		+"getUpdate id: "+updateResult
		+" getMatchedCount : "+updateResult.getMatchedCount() 
		+" getModifiedCount : "+updateResult.getModifiedCount()
		);
   }
   
   public void deleteOwners(String nomCollection, Document filters){

       System.out.println("\n\n\n*********** dans deleteAgents *****************");
       FindIterable<Document> listOwner;
       Iterator it;
       MongoCollection<Document> colOwners=database.getCollection(nomCollection);

       listOwner=colOwners.find(filters).sort(new Document("_id", 1));
       it = listOwner.iterator();// Getting the iterator
       

       colOwners.deleteMany(filters);
       listOwner=colOwners.find(filters).sort(new Document("_id", 1));
       it = listOwner.iterator();
   }
   
   public void getOwner(String nomCollection, 
			Document whereQuery, 
			Document projectionFields,
			Document sortFields){
				
				System.out.println("\n\n\n*********** dans getAgentImmo*****************");   

				MongoCollection<Document> colOwner=database.getCollection(nomCollection);

				FindIterable<Document> listAgentImmo=colOwner.find(whereQuery).sort(sortFields).projection(projectionFields);

				// Getting the iterator 
				Iterator it = listAgentImmo.iterator();
				while(it.hasNext()) {
						System.out.println(it.next());
				}		
		   } 
   
   // creation d'index secondaire 
   
   public void createIndex(String nomCollection, String champs) {
	   
        MongoCollection<Document> colOwners = database.getCollection(nomCollection);

        
        colOwners.createIndex(Indexes.ascending(champs), new IndexOptions().unique(true));
   }

   // Méthodes applicatives de consultation

  

   // Méthode pour trier les Client par un champs precis
   
   public List<Document> getOwnersSortedBy(String champs) {
       MongoCollection<Document> collection = database.getCollection(ownerCollectionName);

       return collection.find()
               .sort(Sorts.ascending(champs))
               .into(new ArrayList<Document>());
   }
// Méthode pour effectuer une agrégation du nombre total de propriétés associées à ce propriétaire
   public AggregateIterable<Document> getTotalProperties(String colProprietes) {
       MongoCollection<Document> ownersCollection = database.getCollection(ownerCollectionName);
       MongoCollection<Document> propertiesCollection = database.getCollection(colProprietes);

       AggregateIterable<Document> result = ownersCollection.aggregate(
               Arrays.asList(
                       Aggregates.match(Filters.eq("_id", "ownerId")),
                       Aggregates.lookup(colProprietes, "_id", "ownerId",colProprietes ),
                       Aggregates.unwind("$accomodation"),
                       Aggregates.group("$_id", Accumulators.sum("totalProperties", 1)),
                       Aggregates.project(Projections.fields(
                               Projections.excludeId(),
                               Projections.include("totalProperties")
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

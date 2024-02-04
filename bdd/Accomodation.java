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

public class Accomodation {
	private MongoDatabase database;
	private String dbName="Agence_immobilière";
	private String hostName="localhost";
	private int port=27017;
	private String userName="yasmine";
	private String passWord="Yasmine7621";
	private String accomodationCollectionName="Accomodation";
   
	public static void main( String args[] ) {  
	    try{
	    	Accomodation Accomodation= new Accomodation();
	    	Accomodation.dropCollectionAccomodation(Accomodation.accomodationCollectionName);
	    	loadJsonToMongoDB("./data/"+Accomodation.accomodationCollectionName+".json", Accomodation.dbName, Accomodation.accomodationCollectionName);
	    	Accomodation.createCollectionAccomodation(Accomodation.accomodationCollectionName);
	    	Accomodation.testInsertOneAccomodation();
	    	Accomodation.getAccomodationsSortedBy("type");
	    	Accomodation.deleteAccomodations(Accomodation.accomodationCollectionName, new Document("_id", "12000"));
	    	Accomodation.getAccomodation(Accomodation.accomodationCollectionName, 
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
	   
   public Accomodation(){
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
   public void createCollectionAccomodation(String nomCollection){
		//Creating a collection 
		database.createCollection(nomCollection); 
		System.out.println("Collection Accomodation created successfully"); 

   }
   
   
   /**
	FV3 : Cette fonction permet de supprimer une collection
	de nom nomCollection.
   */
   
   public void dropCollectionAccomodation(String nomCollection){
		//Drop a collection 
		MongoCollection<Document> colAccomodation=null; 
		

		colAccomodation=database.getCollection(nomCollection);
		System.out.println("!!!! Collection Accomodation : "+colAccomodation);
		// Visiblement jamais !!!
		if (colAccomodation==null)
			System.out.println("Collection inexistante");
		else {
			colAccomodation.drop();	
			System.out.println("Collection Accomodation removed successfully !!!"); 
	  
		}
   }

   /**
	FV4 : Cette fonction permet d'inserer un vol dans une collection.
   */
   
   public void insertOneAccomodation(String nomCollection, Document accomodation){
		//Drop a collection 
		MongoCollection<Document> colAccomodation=database.getCollection(nomCollection);
		colAccomodation.insertOne(accomodation); 
		System.out.println("Document inserted successfully");     
   }

   /**
	FV5 : Cette fonction permet de tester la methode insertOneVol.
   */

   public void testInsertOneAccomodation(){
		Document accomodation =  new Document("_id", "12000")
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
      
		this.insertOneAccomodation(this.accomodationCollectionName, accomodation);
		System.out.println("Document inserted successfully");     
   }

   /**
	FV6 : Cette fonction permet d'inserer plusieurs Vols dans une collection
   */

   public void insertManyAccomodation(String nomCollection, List<Document> colClient){
		//Drop a collection 
		MongoCollection<Document> colAccomodation=database.getCollection(nomCollection);
		colAccomodation.insertMany(colClient); 
		System.out.println("Many Documents inserted successfully");     
   }
   
   /**
	FD10 : Cette fonction permet de modifier des Client dans une collection.
	Le parametre whereQuery : permet de passer des conditions de recherche
	Le parametre updateExpressions : permet d'indiquer les champs a modifier
	Le parametre UpdateOptions : permet d'indiquer les options de mise a jour :
		.upSert : insere si le document n'existe pas
   */
   public void updateAccomodation(String nomCollection, 
	Document whereQuery, 
	Document updateExpressions,
	UpdateOptions updateOptions
	){
		//Drop a collection 
		System.out.println("\n\n\n*********** dans updateAddresses *****************");   

		MongoCollection<Document> colAccomodation=database.getCollection(nomCollection);
		UpdateResult updateResult = colAccomodation.updateMany(whereQuery, updateExpressions);
		
		System.out.println("\nResultat update : "
		+"getUpdate id: "+updateResult
		+" getMatchedCount : "+updateResult.getMatchedCount() 
		+" getModifiedCount : "+updateResult.getModifiedCount()
		);
   }
   
   public void deleteAccomodations(String nomCollection, Document filters){

       System.out.println("\n\n\n*********** dans deleteAgents *****************");
       FindIterable<Document> listAccomodation;
       Iterator it;
       MongoCollection<Document> colAccomodations=database.getCollection(nomCollection);

       listAccomodation=colAccomodations.find(filters).sort(new Document("_id", 1));
       it = listAccomodation.iterator();// Getting the iterator
       

       colAccomodations.deleteMany(filters);
       listAccomodation=colAccomodations.find(filters).sort(new Document("_id", 1));
       it = listAccomodation.iterator();
   }
   
   public void getAccomodation(String nomCollection, 
			Document whereQuery, 
			Document projectionFields,
			Document sortFields){
				
				System.out.println("\n\n\n*********** dans getAgentImmo*****************");   

				MongoCollection<Document> colAccomodation=database.getCollection(nomCollection);

				FindIterable<Document> listAccomodation=colAccomodation.find(whereQuery).sort(sortFields).projection(projectionFields);

				// Getting the iterator 
				Iterator it = listAccomodation.iterator();
				while(it.hasNext()) {
						System.out.println(it.next());
				}		
		   } 
   
   // creation d'index secondaire 
   
   public void createIndex(String nomCollection, String champs) {
	   
        MongoCollection<Document> colAccomodations = database.getCollection(nomCollection);

        
        colAccomodations.createIndex(Indexes.ascending(champs), new IndexOptions().unique(true));
   }

   // Méthodes applicatives de consultation

  

   // Méthode pour trier les Client par un champs precis
   
   public List<Document> getAccomodationsSortedBy(String champs) {
       MongoCollection<Document> collection = database.getCollection(accomodationCollectionName);

       return collection.find()
               .sort(Sorts.ascending(champs))
               .into(new ArrayList<Document>());
   }
   

// Méthode pour récupérer des informations liées à une propriété spécifique
   public AggregateIterable<Document> getPropertyDetails(int propertyId) {
       MongoCollection<Document> accomodationsCollection = database.getCollection("accomodations");
       MongoCollection<Document> ownersCollection = database.getCollection("owners");
       MongoCollection<Document> addressesCollection = database.getCollection("addresses");
       MongoCollection<Document> realEstateAgentsCollection = database.getCollection("real_estate_agents");

       AggregateIterable<Document> result = accomodationsCollection.aggregate(
               Arrays.asList(
                       Aggregates.match(Filters.eq("idAccomodation", propertyId)),
                       Aggregates.lookup("owner", "idOwner", "_id", "owner"),
                       Aggregates.lookup("address", "idAddress", "_id", "address"),
                       Aggregates.lookup("EstateAgent", "idEA", "_id", "EstateAgent"),
                       Aggregates.unwind("$owner"),
                       Aggregates.project(Projections.fields(
                               Projections.excludeId(),
                               Projections.include("number", "type", "Goal", "roomNumber", "floorSpace", "state", "Price", "isAvailable", "AvailabilityDate"),
                               Projections.computed("owner", "$owner"),
                               Projections.computed("address", "$address"),
                               Projections.computed("EstateAgent", "$EstateAgent")
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

package oxigen;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.Properties;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
//dump data from local to azure blob storage
public class DumpDataToBlob {
	public static  String storageConnectionString;
	public static  String containerName;
	public static String DefaultEndpointsProtocol;
	public static String AccountName;
	public static String AccountKey ;
	public static PrintWriter out = null;
	public BlobOutputStream blobOutputStream;
	public ByteArrayInputStream inputStream;
	CloudStorageAccount storageAccount;
	CloudBlobClient blobClient;
	CloudBlobContainer container;
	static CloudBlockBlob blob ;

    public CloudBlockBlob ConnectToAzure(String blobName) throws GeneralSecurityException, URISyntaxException, StorageException, FileNotFoundException, IOException{
		
		 Properties properties = new Properties();
		 properties.load(new FileInputStream("E:\\input\\connectToAzure.properties"));
		       
		  DefaultEndpointsProtocol=properties.getProperty("DefaultEndpointsProtocol");
		  AccountName=properties.getProperty("AccountName");
		  containerName=properties.getProperty("containerName");
		  AccountKey=properties.getProperty("AccountKey");
		  storageConnectionString = 
				    "DefaultEndpointsProtocol="+DefaultEndpointsProtocol+";" +
				    "AccountName="+AccountName+";" + 
				    "AccountKey="+AccountKey;
		  
		  storageAccount = CloudStorageAccount.parse(storageConnectionString);
		  blobClient = storageAccount.createCloudBlobClient();
		  container = blobClient.getContainerReference(containerName);
		  blob = container.getBlockBlobReference(blobName);
		   
		  
		 return blob;
	 }
	public static void main(String[] args) throws FileNotFoundException, GeneralSecurityException, URISyntaxException, StorageException, IOException {
		DumpDataToBlob dump=new DumpDataToBlob();
		blob=dump.ConnectToAzure("facebook_1.csv");
		String path="E:\\input\\facebook_1.csv";
		blob.uploadFromFile(path);

	}

}

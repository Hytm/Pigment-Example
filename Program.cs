using System.Runtime.CompilerServices;
using Aerospike.Client;

AerospikeClient client = new AerospikeClient("172.17.0.2", 3100);

string namespaceName = "test";
string setName = "demo";

string positionBin = "pos"; // Bin to store the position of each chunk
string partialArrayBin = "part"; // Bin to store the partial array chunks

string numberOfChunksBin = "size"; // Bin to store the size of the original record
string listBin = "list"; // Bin to store the list of keys for the chunks

// Client policy to use alternate services to run with Docker container
ClientPolicy clientPolicy = new ClientPolicy();
clientPolicy.useServicesAlternate = true;

try
{
    // Generate a 10 MiB record value from a file
    byte[] value = File.ReadAllBytes("Sample.mp4");
    Console.WriteLine("Original file size: " + value.Length + " bytes");
    
    // Split the record in 4 MiB chunks and store them in a list
    var chunkSize = 1 * 1024 * 1024; // 4 MiB
    int numChunks = (value.Length + chunkSize - 1) / chunkSize;
    byte[][] parts = new byte[numChunks][];
    for (int i = 0; i < numChunks; i++)
    {
        var remaining = value.Length - (i * chunkSize);
        var toCopy = Math.Min(chunkSize, remaining);
        byte[] chunk = new byte[toCopy];
        Array.Copy(value, chunkSize * i, chunk, 0, toCopy);
        parts[i] = chunk;
    }

    // Create a transaction to store the records (metadata + parts) in Aerospike
    Txn txn = new Txn();
    Key metadataKey = new Key(namespaceName, setName, "metadata");

    Record metadataRecord = client.Get(null, metadataKey);

    // If metadata record already exists, prepare for cleaning existing records after transaction
    Key[]? keysToDelete = null;
    if (metadataRecord != null)
    {
        var rawStoredParts = metadataRecord.GetList(listBin) as List<object>; ;
        keysToDelete = GetKeysFromList(rawStoredParts, namespaceName, setName);
    }

    // Start a transaction to store the records in Aerospike 
    try
    {
        WritePolicy writePolicy = client.WritePolicyDefault.Clone();
        writePolicy.Txn = txn;

        // Store each part in a separate record
        List<string> keys = new List<string>();
        for (int i = 0; i < parts.Length; i++)
        {
            Guid uuid = Guid.NewGuid();
            Key k = new Key(namespaceName, setName, uuid.ToString());
            var bins = new Bin[]
            {
                new Bin(partialArrayBin, parts[i]),
                new Bin(positionBin, i)
            };
            client.Put(writePolicy, k, bins);
            keys.Add(uuid.ToString());
        }

        // Store the metadata record in Aerospike
        var metadatBins = new Bin[]
        {
            new Bin(listBin, keys),
            new Bin(numberOfChunksBin, keys.Count)
        };

        client.Put(writePolicy, metadataKey, metadatBins);

        // Commit the transaction
        client.Commit(txn);
    }
    catch (AerospikeException e)
    {
        // Handle any errors during the transaction
        Console.WriteLine("Error during transaction: " + e.Message);
        client.Abort(txn);
    }

    // Clean up existing records if metadata record was found
    CleanExistingRecords(client, keysToDelete);

    // Retrieve the metadata record
    metadataRecord = client.Get(null, metadataKey);
    if (metadataRecord != null)
    {
        // Retrieve the list of keys from the metadata record
        var rawStoredParts = metadataRecord.GetList(listBin) as List<object>; ;
        var keysToRead = GetKeysFromList(rawStoredParts, namespaceName, setName);
        Record[] records = client.Get(null, keysToRead);
        byte[] partsData = GetCombinedStoredParts(records, partialArrayBin, positionBin, metadataRecord.GetInt(numberOfChunksBin));

        //Writing value to file for verification
        string filePath = "Control_Sample.mp4";
        if (File.Exists(filePath))
        {
            File.Delete(filePath); // Delete existing file if it exists
        }
        File.WriteAllBytes(filePath, partsData);
        Console.WriteLine("Data written to file: " + filePath);
    }
    else
    {
        Console.WriteLine("Metadata record not found.");
    }
}
catch (AerospikeException ae)
{
    Console.WriteLine("\nError: " + ae.Message);
}

// Close the client
client.Close();

static Key[]? GetKeysFromList(List<object>? rawStoredParts, string namespaceName, string setName)
{
    List<string> storedParts = new List<string>();
    if (rawStoredParts != null)
    {
        foreach (var part in rawStoredParts)
        {
            if (part is string strPart && strPart != null)
            {
                storedParts.Add(strPart);
            }
        }

        // Retrieve each part using the stored keys in a single batch read
        Key[] keysToRead = new Key[storedParts.Count];
        for (int i = 0; i < storedParts.Count; i++)
        {
            keysToRead[i] = new Key(namespaceName, setName, storedParts[i]);
        }

        return keysToRead;
    }
    else
    {
        Console.WriteLine("No parts found in metadata record.");
        return null;
    }
}

static byte[] GetCombinedStoredParts(Record[] records, string partialArrayBin, string positionBin, int numberOfChunks)
{
    // Validate the number of chunks and ensure order of parts
    Array[] data = new Array[records.Length];
    for (int i = 0; i < records.Length; i++)
    {
        if (records[i] != null)
        {
            byte[]? partData = records[i].GetValue(partialArrayBin) as byte[];
            int position = records[i].GetInt(positionBin);
            if (partData == null)
            {
                Console.WriteLine("Part " + i + " data is null.");
                continue;
            }
            if (partData.Length == 0)
            {
                Console.WriteLine("Part " + i + " data is empty.");
                continue;
            }
            if (position < 0 || position >= numberOfChunks)
            {
                Console.WriteLine("Part " + i + " has an invalid position: " + position);
                continue;
            }
            data[position] = partData;
        }
        else
        {
            Console.WriteLine("Part " + i + " not found.");
            continue;
        }
    }

    if (data.Length != numberOfChunks)
    {
        Console.WriteLine("Mismatch in number of chunks. Expected: " + numberOfChunks + ", Found: " + data.Length);
        return Array.Empty<byte>();
    }

    // Combine all parts into a single byte array
    using (var ms = new MemoryStream())
    {
        foreach (var part in data)
        {
            if (part is byte[] partData && partData.Length > 0)
            {
                ms.Write(partData, 0, partData.Length);
            }
        }
        return ms.ToArray();
    }
}

static void CleanExistingRecords(AerospikeClient client, Key[]? keysToDelete)
{
    WritePolicy deletePolicy = new WritePolicy();
    deletePolicy.Txn = null; // Ensure we are not using a transaction for deletion
    deletePolicy.durableDelete = true; // Ensure durable delete is set

    if (keysToDelete != null)
    {
        foreach (var key in keysToDelete)
        {
            if (key != null)
            {
                try
                {
                    client.Delete(deletePolicy, key);
                }
                catch (AerospikeException e)
                {
                    Console.WriteLine("Error deleting old part: " + e.Message);
                }
            }
        }
    }
}
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;
import com.monitorjbl.xlsx.StreamingReader;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

public class DurableFunctionsSample {

    /**
     * This HTTP-triggered function starts the orchestration.
     * BlobTrigger doesn't stream. Question to customer is can they streamline the initial file-break process, or will they have to load it all in to pre-process it.
     */
    @FunctionName("StartOrchestration")
    public HttpResponseMessage startOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.FUNCTION, route = "blobin/{blobName}") HttpRequestMessage<Optional<String>> request,
            @BindingName(value = "blobName") String blobName,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {

        context.getLogger().info("Java HTTP trigger received request to process blob " + blobName);

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("ProcessWorksheet", blobName);

        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);

        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * This is the orchestrator function, which can schedule activity functions, create durable timers,
     * or wait for external events in a way that's completely fault-tolerant.
     */
    @FunctionName("ProcessWorksheet")
    public long batchOrchestrator(
            @DurableOrchestrationTrigger(name = "taskOrchestrationContext") TaskOrchestrationContext ctx,
            final ExecutionContext context) {

        //start by invoking the activity that will crack the spreadsheet into multiple ones.
        //we will then process each one, and finally aggregate the results together
        String[] smallFiles = ctx.callActivity("BreakLargeWorksheet", ctx.getInput(String.class), String[].class).await();

        List<Task<String>> allActivities = new ArrayList<>();
        for (String smallFile : smallFiles) {
            allActivities.add(ctx.callActivity("ProcessSmallFile", smallFile, String.class));
        }

        context.getLogger().info("Waiting for files to process");
        List<String> allResults = ctx.allOf(allActivities).await();
        context.getLogger().info("All files processed");
        long answer = 0;
        for (String result : allResults) {
            answer += Long.parseLong(result);
        }

        context.getLogger().info("Finished worksheet processing");

        return answer;
    }

    /**
     * This is the activity function that gets invoked by the orchestrator function.
     */
    @FunctionName("BreakLargeWorksheet")
    public String[] breakLargeWorksheet(
            @DurableActivityTrigger(name = "worksheetName") String worksheetName,
//            @BlobInput(connection = "ExcelInBlobs", path = "blobin/{worksheetName}", name = "largeWorksheet", dataType = "binary") byte[] largeWorksheet,
            final ExecutionContext context) {

        context.getLogger().info("Breaking worksheet: " + worksheetName);
        String random = UUID.randomUUID().toString().substring(0,5);

        BlobServiceClient client = new BlobServiceClientBuilder()
                .connectionString(System.getenv("ExcelInBlobs")).buildClient();

        BlobContainerClient blobsIn = client.createBlobContainerIfNotExists("blobin");
        BlobClient inputBlobClient = blobsIn.getBlobClient(worksheetName);

        InputStream is = inputBlobClient.getBlockBlobClient().openInputStream();
        Workbook workbook = StreamingReader.builder()
                .rowCacheSize(100)    // number of rows to keep in memory (defaults to 10)
                .bufferSize(4096)     // buffer size to use when reading InputStream to file (defaults to 1024)
                .open(is);            // InputStream or File for XLSX file (required)

        BlobContainerClient containerClient = client.createBlobContainerIfNotExists("blob-temp");

        StringBuilder currentCellsToAdd = new StringBuilder();
        List<String> createdFiles = new ArrayList<String>();
        int cellsInCurrentBatch = 0;
        final int countPerBatch = 50000;

        for (Sheet sheet : workbook){
            for (Row r : sheet) {
                for (Cell c : r) {
                    currentCellsToAdd.append(c.getStringCellValue());
                    currentCellsToAdd.append('\n');
                    cellsInCurrentBatch++;

                    if (cellsInCurrentBatch > countPerBatch) {
                        //lock the batch, write the file, and carry on
                        int batchCount = createdFiles.size();
                        String newFile = worksheetName + "-" + random + "-" + batchCount + ".values";
                        BlobClient blobClient = containerClient.getBlobClient(newFile);
                        blobClient.upload(BinaryData.fromString(currentCellsToAdd.toString()));
                        createdFiles.add(newFile);
                        cellsInCurrentBatch = 0;
                        currentCellsToAdd = new StringBuilder();
                        context.getLogger().info("Written batch " + batchCount + " of " + worksheetName);
                    }
                }
            }
        }

        //Clean up for last file
        if (cellsInCurrentBatch > 0) {
            //lock the batch, write the file, and carry on
            String newFile = worksheetName + "-" + random + "-" + createdFiles.size() + ".values";
            BlobClient blobClient = containerClient.getBlobClient(newFile);
            blobClient.upload(BinaryData.fromString(currentCellsToAdd.toString()));
            createdFiles.add(newFile);
        }

        context.getLogger().info("Broken batch into " + createdFiles.size() + " for " + worksheetName);
        return createdFiles.toArray(new String[0]);
    }


    /**
     * This is the activity function that gets invoked by the orchestrator function.
     */
    @FunctionName("ProcessSmallFile")
    public String processSmallFile(
            @DurableActivityTrigger(name = "fileName") String fileName,
            @BlobInput(connection = "ExcelInBlobs", path = "blob-temp/{fileName}", name = "fileContents") String fileContents,
            final ExecutionContext context) {

        context.getLogger().info("Summing contents: " + fileName);

        long sum = 0;
        Stream<String> lines = fileContents.lines();
        for (String line : fileContents.lines().toList()) {
            sum += Long.parseLong(line);
        }

        return String.valueOf(sum);

    }
}
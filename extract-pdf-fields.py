import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
const openAIApiKey = Deno.env.get('OPENAI_API_KEY');
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type'
};
serve(async (req)=>{
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    console.log("Handling OPTIONS request");
    return new Response(null, {
      headers: corsHeaders
    });
  }
  try {
    // Clone the request to avoid body already consumed errors
    const reqClone = req.clone();
    // Log the request content type and size for debugging
    console.log("Request headers:", Object.fromEntries(req.headers.entries()));
    console.log("Request method:", req.method);
    console.log("Request URL:", req.url);
    let requestBody;
    try {
      requestBody = await reqClone.json();
      console.log("Request body successfully parsed as JSON");
    } catch (jsonError) {
      console.error("Failed to parse request body as JSON:", jsonError.message);
      const textBody = await req.clone().text();
      console.log("Raw request body (first 1000 chars):", textBody.substring(0, 1000));
      throw new Error(`Invalid JSON in request body: ${jsonError.message}`);
    }
    console.log("Request body keys:", Object.keys(requestBody));
    console.log("Full request body:", JSON.stringify(requestBody).substring(0, 1000) + "...");
    // Check for all possible property names - the frontend might be using different keys
    const pdfs = requestBody.pdfs || requestBody.pdfsBase64 || requestBody.pdfBase64 || [];
    // Log detailed info about the pdfs object
    console.log("PDFs variable type:", typeof pdfs);
    console.log("Is PDFs an array?", Array.isArray(pdfs));
    if (Array.isArray(pdfs)) {
      console.log("Number of PDFs:", pdfs.length);
      if (pdfs.length > 0) {
        console.log("First PDF item type:", typeof pdfs[0]);
        console.log("First PDF item keys:", Object.keys(pdfs[0] || {}));
      }
    } else if (typeof pdfs === 'object') {
      console.log("PDF object keys:", Object.keys(pdfs));
    }
    // Get fieldNames, with fallback
    const fieldNames = requestBody.fieldNames || requestBody.fields || [];
    console.log("Field names:", fieldNames);
    const selectedArea = requestBody.selectedArea || requestBody.area;
    if (selectedArea) {
      console.log("Selected area:", selectedArea);
    }
    // Check if OpenAI API key is available
    if (!openAIApiKey) {
      console.warn("⚠️ No OpenAI API key found in environment variables");
    } else {
      console.log("✓ OpenAI API key is configured");
    }
    // Validation with more detailed error messages
    if (!pdfs) {
      throw new Error('No PDF data found in request. Expected "pdfs", "pdfsBase64", or "pdfBase64" property.');
    }
    if (!Array.isArray(pdfs)) {
      throw new Error(`PDF data is not an array. Found type: ${typeof pdfs}`);
    }
    if (pdfs.length === 0) {
      throw new Error('PDF array is empty. Please provide at least one PDF.');
    }
    // Normalize the PDF data structure for processing
    const normalizedPdfs = pdfs.map((pdf, index)=>{
      console.log(`Processing PDF at index ${index}`);
      // Handle different possible structures
      if (typeof pdf === 'string') {
        console.log(`PDF ${index} is a string (likely base64)`);
        return {
          name: `document-${index + 1}.pdf`,
          pdfBase64: pdf
        };
      } else if (typeof pdf === 'object') {
        console.log(`PDF ${index} is an object with keys:`, Object.keys(pdf));
        // Check for various property name patterns
        const pdfBase64 = pdf.pdfBase64 || pdf.base64 || pdf.data || null;
        const name = pdf.name || pdf.filename || `document-${index + 1}.pdf`;
        if (!pdfBase64) {
          console.warn(`PDF ${index} (${name}) has no base64 data property found`);
        }
        return {
          name,
          pdfBase64
        };
      } else {
        console.warn(`PDF ${index} has unexpected type: ${typeof pdf}`);
        return {
          name: `document-${index + 1}.pdf`,
          pdfBase64: null
        };
      }
    });
    console.log("Normalized PDFs structure:", normalizedPdfs.map((p)=>({
        name: p.name,
        hasData: !!p.pdfBase64
      })));
    // Process PDFs with OpenAI if API key is available
    let results = [];
    if (openAIApiKey && normalizedPdfs.some((pdf)=>pdf.pdfBase64)) {
      console.log("🔄 Attempting to process PDFs with OpenAI");
      try {
        results = await Promise.all(normalizedPdfs.map(async (pdf, index)=>{
          if (!pdf.pdfBase64) {
            console.warn(`PDF ${index} (${pdf.name}) has no data, skipping OpenAI processing`);
            return {
              name: pdf.name,
              success: false,
              error: "No PDF data provided for this file",
              isUsingMockData: true,
              extractedFields: {}
            };
          }
          console.log(`📄 Starting OpenAI processing for PDF ${index}: ${pdf.name}`);
          try {
            // Use the actual OpenAI processing function
            const extractedFields = await extractDataWithAssistant(Uint8Array.from(atob(pdf.pdfBase64), (c)=>c.charCodeAt(0)), fieldNames, pdf.name);
            console.log(`✅ OpenAI processing successful for ${pdf.name}`);
            console.log(`Extracted fields for ${pdf.name}:`, JSON.stringify(extractedFields).substring(0, 200) + "...");
            return {
              name: pdf.name,
              success: true,
              extractedFields,
              isUsingMockData: false
            };
          } catch (pdfError) {
            console.error(`❌ OpenAI processing failed for ${pdf.name}:`, pdfError);
            console.error(`Error stack for ${pdf.name}:`, pdfError.stack);
            // Generate mock data as fallback
            console.log(`⚠️ Falling back to mock data for ${pdf.name}`);
            const mockFields = getMockFields({
              fieldNames,
              selectedArea,
              pdfName: pdf.name
            });
            return {
              name: pdf.name,
              success: false,
              error: pdfError.message,
              extractedFields: mockFields,
              isUsingMockData: true
            };
          }
        }));
        console.log(`✅ Completed OpenAI processing for ${results.filter((r)=>!r.isUsingMockData).length} PDFs successfully`);
        console.log(`⚠️ Used mock data for ${results.filter((r)=>r.isUsingMockData).length} PDFs`);
      } catch (batchError) {
        console.error("❌ Batch processing with OpenAI failed:", batchError);
        console.error("Batch error stack:", batchError.stack);
      // Will fall back to mock data for all PDFs
      }
    } else {
      console.log("⚠️ No OpenAI API key or valid PDFs, using mock data for all PDFs");
    }
    // If OpenAI processing wasn't done or completely failed, generate mock data for all PDFs
    if (results.length === 0) {
      console.log("Generating mock data for all PDFs");
      results = normalizedPdfs.map((pdf, index)=>{
        console.log(`Generating mock results for PDF ${index}: ${pdf.name}`);
        // Check if this PDF has data
        if (!pdf.pdfBase64) {
          console.warn(`PDF ${index} (${pdf.name}) has no base64 data, returning error result`);
          return {
            name: pdf.name,
            success: false,
            error: "No PDF data provided for this file",
            isUsingMockData: true,
            extractedFields: {}
          };
        }
        // Generate mock data for this PDF
        const mockFields = getMockFields({
          fieldNames,
          selectedArea,
          pdfName: pdf.name
        });
        return {
          name: pdf.name,
          success: true,
          extractedFields: mockFields,
          isUsingMockData: true
        };
      });
    }
    console.log(`Generated ${results.length} total results`);
    console.log("First result sample:", JSON.stringify(results[0]).substring(0, 200) + "...");
    const response = {
      results,
      totalProcessed: pdfs.length,
      successCount: results.filter((r)=>r.success).length,
      isUsingMockData: results.some((r)=>r.isUsingMockData)
    };
    console.log("Sending response with structure:", Object.keys(response));
    console.log(`Response includes ${response.results.length} results, ${response.successCount} successful`);
    // Add this right before the final return statement in your server code
    // Place it just before this line:
    // return new Response(JSON.stringify(response), {
    // Create a detailed log of the final response
    console.log("🔄 PREPARING FINAL RESPONSE TO CLIENT 🔄");
    console.log("====================================================");
    console.log("✨ COMPLETE RESPONSE STRUCTURE:");
    console.log(JSON.stringify(response, null, 2));
    console.log("====================================================");
    console.log("📊 SUMMARY:");
    console.log(`Total PDFs processed: ${response.totalProcessed}`);
    console.log(`Successfully processed: ${response.successCount}`);
    console.log(`Using mock data: ${response.isUsingMockData}`);
    console.log("====================================================");
    console.log("📄 RESULTS BY DOCUMENT:");
    // Log each document result individually for better readability
    response.results.forEach((result, index)=>{
      console.log(`\n--- DOCUMENT ${index + 1}: ${result.name} ---`);
      console.log(`Success: ${result.success}`);
      console.log(`Using mock data: ${result.isUsingMockData}`);
      console.log("Extracted fields:");
      console.log(JSON.stringify(result.extractedFields, null, 2));
      if (result.error) {
        console.log(`Error: ${result.error}`);
      }
    });
    console.log("\n====================================================");
    console.log("🚀 SENDING RESPONSE TO CLIENT 🚀");
    console.log(JSON.stringify(response));
    return new Response(JSON.stringify(response), {
      headers: {
        ...corsHeaders,
        'Content-Type': 'application/json'
      }
    });
  } catch (error) {
    console.error('Error processing request:', error);
    console.error('Error stack:', error.stack);
    // Return error with more details
    return new Response(JSON.stringify({
      error: error.message,
      stack: error.stack,
      isUsingMockData: true,
      results: []
    }), {
      headers: {
        ...corsHeaders,
        'Content-Type': 'application/json'
      },
      status: 400
    });
  }
});
async function extractDataWithAssistant(pdfBuffer, fieldNames, documentName = 'document.pdf') {
  const openAIHeaders = {
    'Authorization': `Bearer ${openAIApiKey}`,
    'Content-Type': 'application/json',
    'OpenAI-Beta': 'assistants=v2'
  };
  let fileId = null;
  try {
    // Upload PDF
    const fileFormData = createFormData('file', pdfBuffer, documentName, 'assistants');
    const fileResponse = await fetch('https://api.openai.com/v1/files', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${openAIApiKey}`,
        'OpenAI-Beta': 'assistants=v2'
      },
      body: fileFormData
    });
    const fileResponseText = await fileResponse.text();
    const fileData = JSON.parse(fileResponseText);
    if (fileData.error) throw new Error(`File upload error: ${fileData.error.message}`);
    fileId = fileData.id;
    // Create Thread
    const threadResponse = await fetch('https://api.openai.com/v1/threads', {
      method: 'POST',
      headers: openAIHeaders,
      body: JSON.stringify({})
    });
    const threadData = await threadResponse.json();
    const threadId = threadData.id;
    // 🧠 Context-aware instructions using your exact field names
    const instructions = `
You are a smart document extraction assistant specialized in processing ecommerce invoices and delivery challans.

You are provided with a list of field names that a client expects as output. These field names may use different phrasing than what appears in the document — your job is to understand what values they represent based on your domain knowledge and context from the document.

Each JSON object you output must represent one **line item** from the invoice and include **all fields exactly as provided** in the list below.

🧠 Use contextual reasoning to match fields. For example:
- "GSTIN Shipper" may appear as "GSTIN" under "Ship from"
- "fba number" may appear as "FBA Shipment ID"
- "hsn" refers to HSN/SAC codes
- "product description" refers to item/product details
- "total value" refers to the final amount per line item including taxes
- "doc attached" should be the name of the document processed (provided separately)

🚫 Do not rename or invent any field names. Output keys **must exactly match** the list below.

📌 Provided field names:
\`\`\`json
${JSON.stringify(fieldNames, null, 2)}
\`\`\`

📄 Output a JSON array:
- One object per line item.
- If a field value is not found, use "" for strings and 0 for numbers.
- Include document-level values (like GSTIN Shipper, fba number) in every line item.
- Use "${documentName}" for the "doc attached" field.

✅ Output format:
\`\`\`json
[
  {
    "GSTIN Shipper": "string",
    "fba number": "string",
    "hsn": "string",
    "product description": "string",
    "total value": number,
    "doc attached": "string"
  }
]
\`\`\`

Return ONLY the JSON. No explanation or formatting outside the JSON block.
`;
    // Create Assistant
    const assistantResponse = await fetch('https://api.openai.com/v1/assistants', {
      method: 'POST',
      headers: openAIHeaders,
      body: JSON.stringify({
        name: "PDF Line Item Extractor",
        instructions,
        model: "gpt-4-turbo",
        tools: [
          {
            type: "file_search"
          }
        ]
      })
    });
    const assistantData = await assistantResponse.json();
    const assistantId = assistantData.id;
    // Send Message
    await fetch(`https://api.openai.com/v1/threads/${threadId}/messages`, {
      method: 'POST',
      headers: openAIHeaders,
      body: JSON.stringify({
        role: "user",
        content: `Please extract data based on provided fields from this PDF: ${documentName}`,
        attachments: [
          {
            file_id: fileId,
            tools: [
              {
                type: "file_search"
              }
            ]
          }
        ]
      })
    });
    // Run Assistant
    const runResponse = await fetch(`https://api.openai.com/v1/threads/${threadId}/runs`, {
      method: 'POST',
      headers: openAIHeaders,
      body: JSON.stringify({
        assistant_id: assistantId
      })
    });
    const runData = await runResponse.json();
    const runId = runData.id;
    // Poll until complete
    while(true){
      const statusRes = await fetch(`https://api.openai.com/v1/threads/${threadId}/runs/${runId}`, {
        method: 'GET',
        headers: openAIHeaders
      });
      const statusData = await statusRes.json();
      if (statusData.status === "completed") break;
      if ([
        "failed",
        "cancelled",
        "expired"
      ].includes(statusData.status)) {
        throw new Error(`Run ${statusData.status}: ${statusData.last_error?.message || "Unknown error"}`);
      }
      await new Promise((r)=>setTimeout(r, 3000));
    }
    // Get Messages
    const messagesRes = await fetch(`https://api.openai.com/v1/threads/${threadId}/messages`, {
      method: 'GET',
      headers: openAIHeaders
    });
    const messagesData = await messagesRes.json();
    const assistantMessage = messagesData.data.find((m)=>m.role === "assistant" && m.run_id === runId);
    const extractedText = assistantMessage?.content?.[0]?.text?.value?.trim();
    if (!extractedText) throw new Error("Assistant returned empty response");
    // Parse JSON Array
    const jsonMatch = extractedText.match(/```json\n([\s\S]*?)\n```/);
    const jsonStr = jsonMatch ? jsonMatch[1] : extractedText;
    const parsedData = JSON.parse(jsonStr);
    // Rename per line item
    return parsedData.map((item, index)=>{
      const suffixName = documentName.replace(/\.pdf$/, `_${index + 1}.json`);
      return {
        filename: suffixName,
        data: item
      };
    });
  } catch (error) {
    console.error(`❌ Error processing ${documentName}:`, error);
    throw error;
  } finally{
    if (fileId) {
      try {
        await fetch(`https://api.openai.com/v1/files/${fileId}`, {
          method: 'DELETE',
          headers: {
            'Authorization': `Bearer ${openAIApiKey}`,
            'OpenAI-Beta': 'assistants=v2'
          }
        });
      } catch (deleteError) {
        console.error(`❌ Error deleting file ${fileId}:`, deleteError.message);
      }
    }
  }
}
// Function to create FormData for file upload
function createFormData(fieldName, fileData, fileName, purpose) {
  console.log(`📋 Creating form data for ${fileName} (${fileData.length} bytes)`);
  const formData = new FormData();
  const blob = new Blob([
    fileData
  ], {
    type: 'application/pdf'
  });
  formData.append(fieldName, blob, fileName);
  formData.append('purpose', purpose);
  return formData;
}
// Enhanced mock data generator that preserves original structure
const getMockFields = ({ fieldNames, selectedArea, pdfName = "unknown.pdf" })=>{
  console.log(`Generating mock fields for ${pdfName}`);
  console.log(`Requested fields: ${JSON.stringify(fieldNames)}`);
  // Generate slightly different mock data for each PDF to demonstrate multiple files working
  const pdfNumber = pdfName.match(/\d+/) ? pdfName.match(/\d+/)[0] : "X";
  // Create different mock data based on the file number
  // Use even numbers for multi-line invoices, odd for single-line
  const useMultipleItems = pdfNumber % 2 === 0;
  let allMockFields;
  if (useMultipleItems) {
    // Multi-line invoice with lineItems array
    const lineItem1 = {
      productDescription: `AutoBizarre Anti-Theft Heavy Duty Metal Body Adjustable Claw Wheel Security Lock (${pdfNumber})`,
      quantity: 78,
      unitValue: 677.97,
      hsnSacCode: "8301",
      taxableValue: 52881.36,
      taxRate: 18.00,
      taxValue: 9518.64,
      totalValue: 62400.00
    };
    const lineItem2 = {
      productDescription: `AutoBizarre Car Interior Blue Star Roof Projector Ambient Light (${pdfNumber})`,
      quantity: 30,
      unitValue: 321.19,
      hsnSacCode: "8512",
      taxableValue: 9635.59,
      taxRate: 18.00,
      taxValue: 1734.41,
      totalValue: 11370.00
    };
    const lineItems = [
      lineItem1,
      lineItem2
    ];
    const totalAmount = lineItems.reduce((sum, item)=>sum + item.totalValue, 0);
    const totalTaxableValue = lineItems.reduce((sum, item)=>sum + item.taxableValue, 0);
    const totalTaxValue = lineItems.reduce((sum, item)=>sum + item.taxValue, 0);
    allMockFields = {
      "Document Number": `FBA15K${pdfNumber}N1JKF`,
      "Document Date": "2025-02-27",
      "FBA Shipment ID": `FBA15K${pdfNumber}N1JKF`,
      "Purpose of transfer": "Stock Transfer",
      "Number of box": 1,
      "Supplier Name": "AI Enterprises",
      "Supplier Address": "1043 K-1 Ward No.8, Mehrauli New Delhi - 110030",
      "Supplier GSTIN": "07BLZPA4905P1ZF",
      "Ship To": "Amazon Seller Services Private Limited",
      "Ship To Address": "ESR Sohna Logistics Park, Village Rahaka, HARYANA",
      "Ship To GSTIN": "06BLZPA4905P1ZH",
      "Place of supply": "HARYANA (State/UT Code: 6)",
      "Place of delivery": "HARYANA (State/UT Code: 6)",
      "Total Taxable Value": totalTaxableValue,
      "Total IGST Value": totalTaxValue,
      "Total Value": totalAmount,
      "lineItems": lineItems
    };
  } else {
    // Single-line invoice with fields at the top level (original structure)
    allMockFields = {
      "Document Number": `FBA15K${pdfNumber}F2R63`,
      "Document Date": "2025-02-26",
      "FBA Shipment ID": `FBA15K${pdfNumber}F2R63`,
      "Purpose of transfer": "Stock Transfer",
      "Number of box": 13,
      "Supplier Name": "AI Enterprises",
      "Supplier Address": "1043 K-1 Ward No.8, Mehrauli New Delhi - 110030",
      "Supplier GSTIN": "07BLZPA4905P1ZF",
      "Ship To": "Amazon Seller Services Private Limited",
      "Ship To Address": "Emporium Industrial Park India Pvt Ltd, Village Rahaka, HARYANA",
      "Ship To GSTIN": "06BLZPA4905P1ZH",
      "Place of supply": "HARYANA (State/UT Code: 6)",
      "Place of delivery": "HARYANA (State/UT Code: 6)",
      "productDescription": `AutoBizarre Anti-Theft Heavy Duty Metal Body Adjustable Claw Wheel Security Lock (${pdfNumber})`,
      "quantity": 78,
      "unitValue": 677.97,
      "hsnSacCode": "8301",
      "taxableValue": 52881.36,
      "taxRate": 18.00,
      "taxValue": 9518.64,
      "totalValue": 62400.00,
      "Total Taxable Value": 52881.36,
      "Total IGST Value": 9518.64,
      "Total Value": 62400.00
    };
  }
  // If specific fields are requested, only return those
  if (fieldNames && fieldNames.length > 0) {
    const requestedFields = {};
    fieldNames.forEach((field)=>{
      requestedFields[field] = allMockFields[field] || `Sample ${field}`;
    });
    console.log(`Returning ${Object.keys(requestedFields).length} specific fields`);
    return requestedFields;
  }
  console.log(`Returning all fields for ${pdfName}`);
  return allMockFields;
};


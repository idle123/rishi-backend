import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const openAIApiKey = Deno.env.get('OPENAI_API_KEY');
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type'
};

// Configuration for batch processing
const CONFIG = {
  BATCH_SIZE: 5, // Process 5 PDFs concurrently
  MAX_RETRIES: 3,
  INITIAL_DELAY: 1000, // 1 second
  MAX_DELAY: 30000, // 30 seconds
  RATE_LIMIT_DELAY: 2000, // 2 seconds between API calls
  MAX_FILE_SIZE: 512 * 1024 * 1024, // 512MB per file
  MAX_FILES_PER_REQUEST: 100
};

// Global assistant to reuse across requests
let globalAssistant = null;
let assistantCreationPromise = null;

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    console.log("Handling OPTIONS request");
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const requestBody = await parseRequestBody(req);
    const { pdfs, fieldNames, selectedArea } = validateAndNormalizeRequest(requestBody);
    
    console.log(`🚀 Processing ${pdfs.length} PDFs with batch processing`);
    
    // Initialize or reuse global assistant
    if (!globalAssistant && openAIApiKey) {
      globalAssistant = await getOrCreateAssistant(fieldNames);
    }

    // Process PDFs in batches
    const results = await processPDFsInBatches(pdfs, fieldNames, selectedArea);
    
    const response = {
      results,
      totalProcessed: pdfs.length,
      successCount: results.filter(r => r.success).length,
      isUsingMockData: results.some(r => r.isUsingMockData),
      batchInfo: {
        batchSize: CONFIG.BATCH_SIZE,
        totalBatches: Math.ceil(pdfs.length / CONFIG.BATCH_SIZE)
      }
    };

    console.log(`✅ Completed processing: ${response.successCount}/${response.totalProcessed} successful`);
    
    return new Response(JSON.stringify(response), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });

  } catch (error) {
    console.error('❌ Error processing request:', error);
    return new Response(JSON.stringify({
      error: error.message,
      isUsingMockData: true,
      results: []
    }), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      status: 400
    });
  }
});

async function parseRequestBody(req) {
  try {
    const reqClone = req.clone();
    const requestBody = await reqClone.json();
    console.log("✅ Request body parsed successfully");
    return requestBody;
  } catch (jsonError) {
    const textBody = await req.clone().text();
    console.log("Raw request body (first 500 chars):", textBody.substring(0, 500));
    throw new Error(`Invalid JSON in request body: ${jsonError.message}`);
  }
}

function validateAndNormalizeRequest(requestBody) {
  const pdfs = requestBody.pdfs || requestBody.pdfsBase64 || requestBody.pdfBase64 || [];
  const fieldNames = requestBody.fieldNames || requestBody.fields || [];
  const selectedArea = requestBody.selectedArea || requestBody.area;

  if (!Array.isArray(pdfs)) {
    throw new Error(`PDF data is not an array. Found type: ${typeof pdfs}`);
  }

  if (pdfs.length === 0) {
    throw new Error('PDF array is empty. Please provide at least one PDF.');
  }

  if (pdfs.length > CONFIG.MAX_FILES_PER_REQUEST) {
    throw new Error(`Too many files. Maximum ${CONFIG.MAX_FILES_PER_REQUEST} files allowed per request.`);
  }

  // Normalize PDF structure
  const normalizedPdfs = pdfs.map((pdf, index) => {
    if (typeof pdf === 'string') {
      return {
        name: `document-${index + 1}.pdf`,
        pdfBase64: pdf
      };
    } else if (typeof pdf === 'object') {
      const pdfBase64 = pdf.pdfBase64 || pdf.base64 || pdf.data || null;
      const name = pdf.name || pdf.filename || `document-${index + 1}.pdf`;
      
      // Validate file size
      if (pdfBase64) {
        const sizeBytes = Math.ceil(pdfBase64.length * 0.75); // Approximate decoded size
        if (sizeBytes > CONFIG.MAX_FILE_SIZE) {
          console.warn(`PDF ${name} exceeds size limit: ${sizeBytes} bytes`);
        }
      }
      
      return { name, pdfBase64 };
    }
    return {
      name: `document-${index + 1}.pdf`,
      pdfBase64: null
    };
  });

  console.log(`📋 Normalized ${normalizedPdfs.length} PDFs for processing`);
  return { pdfs: normalizedPdfs, fieldNames, selectedArea };
}

async function processPDFsInBatches(pdfs, fieldNames, selectedArea) {
  const allResults = [];
  const totalBatches = Math.ceil(pdfs.length / CONFIG.BATCH_SIZE);
  
  for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
    const startIndex = batchIndex * CONFIG.BATCH_SIZE;
    const endIndex = Math.min(startIndex + CONFIG.BATCH_SIZE, pdfs.length);
    const batch = pdfs.slice(startIndex, endIndex);
    
    console.log(`📦 Processing batch ${batchIndex + 1}/${totalBatches} (${batch.length} PDFs)`);
    
    try {
      const batchResults = await processBatch(batch, fieldNames, selectedArea, batchIndex);
      allResults.push(...batchResults);
      
      // Add delay between batches to respect rate limits
      if (batchIndex < totalBatches - 1) {
        await delay(CONFIG.RATE_LIMIT_DELAY);
      }
      
    } catch (batchError) {
      console.error(`❌ Batch ${batchIndex + 1} failed:`, batchError);
      
      // Generate mock data for failed batch
      const mockResults = batch.map(pdf => ({
        name: pdf.name,
        success: false,
        error: `Batch processing failed: ${batchError.message}`,
        extractedFields: getMockFields({ fieldNames, selectedArea, pdfName: pdf.name }),
        isUsingMockData: true
      }));
      
      allResults.push(...mockResults);
    }
  }
  
  return allResults;
}

async function processBatch(batch, fieldNames, selectedArea, batchIndex) {
  if (!openAIApiKey) {
    console.log(`⚠️ No OpenAI API key, using mock data for batch ${batchIndex + 1}`);
    return batch.map(pdf => ({
      name: pdf.name,
      success: true,
      extractedFields: getMockFields({ fieldNames, selectedArea, pdfName: pdf.name }),
      isUsingMockData: true
    }));
  }

  // Process PDFs in the batch with controlled concurrency
  const batchPromises = batch.map(async (pdf, index) => {
    // Stagger API calls within the batch
    await delay(index * 500);
    
    if (!pdf.pdfBase64) {
      return {
        name: pdf.name,
        success: false,
        error: "No PDF data provided for this file",
        isUsingMockData: true,
        extractedFields: {}
      };
    }

    try {
      const extractedFields = await extractDataWithRetry(pdf, fieldNames);
      return {
        name: pdf.name,
        success: true,
        extractedFields,
        isUsingMockData: false
      };
    } catch (error) {
      console.error(`❌ Failed to process ${pdf.name}:`, error);
      return {
        name: pdf.name,
        success: false,
        error: error.message,
        extractedFields: getMockFields({ fieldNames, selectedArea, pdfName: pdf.name }),
        isUsingMockData: true
      };
    }
  });

  return await Promise.all(batchPromises);
}

async function extractDataWithRetry(pdf, fieldNames) {
  let lastError;
  
  for (let attempt = 0; attempt < CONFIG.MAX_RETRIES; attempt++) {
    try {
      if (attempt > 0) {
        const delayMs = Math.min(
          CONFIG.INITIAL_DELAY * Math.pow(2, attempt - 1),
          CONFIG.MAX_DELAY
        );
        console.log(`🔄 Retry attempt ${attempt + 1} for ${pdf.name} after ${delayMs}ms`);
        await delay(delayMs);
      }
      
      return await extractDataWithAssistant(pdf, fieldNames);
    } catch (error) {
      lastError = error;
      
      // Don't retry on certain errors
      if (error.message.includes('file too large') || 
          error.message.includes('invalid file format')) {
        throw error;
      }
      
      console.warn(`⚠️ Attempt ${attempt + 1} failed for ${pdf.name}: ${error.message}`);
    }
  }
  
  throw lastError;
}

async function getOrCreateAssistant(fieldNames) {
  // Prevent multiple assistant creation attempts
  if (assistantCreationPromise) {
    return await assistantCreationPromise;
  }

  assistantCreationPromise = createOptimizedAssistant(fieldNames);
  try {
    const assistant = await assistantCreationPromise;
    assistantCreationPromise = null;
    return assistant;
  } catch (error) {
    assistantCreationPromise = null;
    throw error;
  }
}

async function createOptimizedAssistant(fieldNames) {
  const instructions = `Extract invoice data as JSON array. Each object = one line item.

Required fields: ${JSON.stringify(fieldNames)}

Rules:
- Use exact field names provided
- Missing values: "" for strings, 0 for numbers  
- Include document-level data in each line item
- Return only JSON array, no explanations

Format:
[{"field1":"value1","field2":"value2"}]`;

  const response = await fetchWithRetry('https://api.openai.com/v1/assistants', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${openAIApiKey}`,
      'Content-Type': 'application/json',
      'OpenAI-Beta': 'assistants=v2'
    },
    body: JSON.stringify({
      name: "Batch Invoice Extractor",
      instructions,
      model: "gpt-4o-mini", // More cost-effective for batch processing
      tools: [{ type: "file_search" }]
    })
  });

  const data = await response.json();
  if (data.error) throw new Error(`Assistant creation failed: ${data.error.message}`);
  
  console.log(`✅ Created reusable assistant: ${data.id}`);
  return data;
}

async function extractDataWithAssistant(pdf, fieldNames) {
  const pdfBuffer = Uint8Array.from(atob(pdf.pdfBase64), c => c.charCodeAt(0));
  let fileId = null;
  let threadId = null;

  try {
    // Upload file
    fileId = await uploadFile(pdfBuffer, pdf.name);
    
    // Create thread
    threadId = await createThread();
    
    // Process with assistant
    const result = await processWithAssistant(threadId, fileId, pdf.name);
    
    return result.map((item, index) => ({
      filename: pdf.name.replace(/\.pdf$/, `_${index + 1}.json`),
      data: item
    }));

  } finally {
    // Cleanup resources
    await cleanupResources(fileId, threadId);
  }
}

async function uploadFile(pdfBuffer, fileName) {
  const formData = new FormData();
  const blob = new Blob([pdfBuffer], { type: 'application/pdf' });
  formData.append('file', blob, fileName);
  formData.append('purpose', 'assistants');

  const response = await fetchWithRetry('https://api.openai.com/v1/files', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${openAIApiKey}`,
      'OpenAI-Beta': 'assistants=v2'
    },
    body: formData
  });

  const data = await response.json();
  if (data.error) throw new Error(`File upload failed: ${data.error.message}`);
  
  return data.id;
}

async function createThread() {
  const response = await fetchWithRetry('https://api.openai.com/v1/threads', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${openAIApiKey}`,
      'Content-Type': 'application/json',
      'OpenAI-Beta': 'assistants=v2'
    },
    body: JSON.stringify({})
  });

  const data = await response.json();
  if (data.error) throw new Error(`Thread creation failed: ${data.error.message}`);
  
  return data.id;
}

async function processWithAssistant(threadId, fileId, fileName) {
  // Send message
  await fetchWithRetry(`https://api.openai.com/v1/threads/${threadId}/messages`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${openAIApiKey}`,
      'Content-Type': 'application/json',
      'OpenAI-Beta': 'assistants=v2'
    },
    body: JSON.stringify({
      role: "user",
      content: `Extract data from: ${fileName}`,
      attachments: [{
        file_id: fileId,
        tools: [{ type: "file_search" }]
      }]
    })
  });

  // Run assistant
  const runResponse = await fetchWithRetry(`https://api.openai.com/v1/threads/${threadId}/runs`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${openAIApiKey}`,
      'Content-Type': 'application/json',
      'OpenAI-Beta': 'assistants=v2'
    },
    body: JSON.stringify({
      assistant_id: globalAssistant.id
    })
  });

  const runData = await runResponse.json();
  if (runData.error) throw new Error(`Run creation failed: ${runData.error.message}`);

  // Poll for completion with timeout
  const runId = runData.id;
  const maxWaitTime = 300000; // 5 minutes
  const pollInterval = 3000; // 3 seconds
  let totalWaitTime = 0;

  while (totalWaitTime < maxWaitTime) {
    const statusRes = await fetchWithRetry(`https://api.openai.com/v1/threads/${threadId}/runs/${runId}`, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${openAIApiKey}`,
        'OpenAI-Beta': 'assistants=v2'
      }
    });

    const statusData = await statusRes.json();
    
    if (statusData.status === "completed") break;
    if (["failed", "cancelled", "expired"].includes(statusData.status)) {
      throw new Error(`Run ${statusData.status}: ${statusData.last_error?.message || "Unknown error"}`);
    }

    await delay(pollInterval);
    totalWaitTime += pollInterval;
  }

  if (totalWaitTime >= maxWaitTime) {
    throw new Error("Processing timeout exceeded");
  }

  // Get result
  const messagesRes = await fetchWithRetry(`https://api.openai.com/v1/threads/${threadId}/messages`, {
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${openAIApiKey}`,
      'OpenAI-Beta': 'assistants=v2'
    }
  });

  const messagesData = await messagesRes.json();
  const assistantMessage = messagesData.data.find(m => m.role === "assistant");
  const extractedText = assistantMessage?.content?.[0]?.text?.value?.trim();

  if (!extractedText) throw new Error("Assistant returned empty response");

  // Parse JSON
  const jsonMatch = extractedText.match(/```json\n([\s\S]*?)\n```/) || extractedText.match(/\[[\s\S]*\]/);
  const jsonStr = jsonMatch ? (jsonMatch[1] || jsonMatch[0]) : extractedText;
  
  try {
    return JSON.parse(jsonStr);
  } catch (parseError) {
    console.error("Failed to parse JSON:", jsonStr);
    throw new Error(`Invalid JSON response: ${parseError.message}`);
  }
}

async function cleanupResources(fileId, threadId) {
  const cleanupPromises = [];

  if (fileId) {
    cleanupPromises.push(
      fetchWithRetry(`https://api.openai.com/v1/files/${fileId}`, {
        method: 'DELETE',
        headers: {
          'Authorization': `Bearer ${openAIApiKey}`,
          'OpenAI-Beta': 'assistants=v2'
        }
      }).catch(err => console.warn(`File cleanup failed: ${err.message}`))
    );
  }

  // Note: Threads auto-cleanup, but you can delete if needed
  // if (threadId) {
  //   cleanupPromises.push(deleteThread(threadId));
  // }

  await Promise.allSettled(cleanupPromises);
}

async function fetchWithRetry(url, options, maxRetries = 3) {
  let lastError;
  
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const response = await fetch(url, options);
      
      // Handle rate limiting
      if (response.status === 429) {
        const retryAfter = response.headers.get('retry-after');
        const delayMs = retryAfter ? parseInt(retryAfter) * 1000 : CONFIG.RATE_LIMIT_DELAY * (attempt + 1);
        console.warn(`Rate limited, waiting ${delayMs}ms before retry`);
        await delay(delayMs);
        continue;
      }
      
      if (!response.ok && response.status >= 500) {
        throw new Error(`Server error: ${response.status}`);
      }
      
      return response;
    } catch (error) {
      lastError = error;
      if (attempt < maxRetries - 1) {
        const delayMs = CONFIG.INITIAL_DELAY * Math.pow(2, attempt);
        console.warn(`Fetch attempt ${attempt + 1} failed, retrying in ${delayMs}ms`);
        await delay(delayMs);
      }
    }
  }
  
  throw lastError;
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Enhanced mock data generator
function getMockFields({ fieldNames, selectedArea, pdfName = "unknown.pdf" }) {
  const pdfNumber = pdfName.match(/\d+/)?.[0] || Math.floor(Math.random() * 1000);
  const useMultipleItems = pdfNumber % 2 === 0;

  const mockData = {
    "Document Number": `FBA15K${pdfNumber}N1JKF`,
    "Document Date": new Date().toISOString().split('T')[0],
    "FBA Shipment ID": `FBA15K${pdfNumber}N1JKF`,
    "Purpose of transfer": "Stock Transfer",
    "Number of box": Math.floor(Math.random() * 20) + 1,
    "Supplier Name": "AI Enterprises",
    "Supplier Address": "1043 K-1 Ward No.8, Mehrauli New Delhi - 110030",
    "Supplier GSTIN": "07BLZPA4905P1ZF",
    "Ship To": "Amazon Seller Services Private Limited",
    "Ship To Address": "ESR Sohna Logistics Park, Village Rahaka, HARYANA",
    "Ship To GSTIN": "06BLZPA4905P1ZH",
    "Place of supply": "HARYANA (State/UT Code: 6)",
    "Place of delivery": "HARYANA (State/UT Code: 6)",
    "productDescription": `Sample Product ${pdfNumber}`,
    "quantity": Math.floor(Math.random() * 100) + 1,
    "unitValue": Math.round((Math.random() * 1000 + 100) * 100) / 100,
    "hsnSacCode": "8301",
    "taxableValue": Math.round((Math.random() * 50000 + 10000) * 100) / 100,
    "taxRate": 18.00,
    "taxValue": 0,
    "totalValue": 0
  };

  // Calculate dependent values
  mockData.taxValue = Math.round(mockData.taxableValue * mockData.taxRate / 100 * 100) / 100;
  mockData.totalValue = mockData.taxableValue + mockData.taxValue;
  
  // Return only requested fields if specified
  if (fieldNames?.length > 0) {
    const result = {};
    fieldNames.forEach(field => {
      result[field] = mockData[field] || `Sample ${field}`;
    });
    return result;
  }

  return mockData;
}

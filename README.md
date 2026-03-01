# Grocery Receipt Scanner

Scan UK grocery receipts on your Android phone → automatic category matching → Excel export → Power BI.

**Estimated cost: ~£1.50/month** (Textract is the only real cost at 100 receipts/month)

---

## What Gets Created on AWS

Running `.\deploy.ps1 deploy` creates all of this in your AWS account:

| Resource | What it is |
|---|---|
| 2x S3 buckets | One for receipt photos, one for Excel exports |
| 5x DynamoDB tables | Receipts, items, categories, retailers, learned mappings |
| 5x Lambda functions | Upload, processor, receipts CRUD, export, reference data |
| 1x API Gateway | The URL your phone calls, secured with an API key |
| 1x IAM Role | Permissions for the Lambdas (scoped, least privilege) |
| 5x CloudWatch log groups | Logs for each Lambda, kept 14 days |

No SQS, no EventBridge, no KMS, no X-Ray — stripped back to only what's needed.

---

## Architecture

```
Android PWA
    |
    | POST /receipts/upload-url
    v
API Gateway --> UploadFunction
                    |
                    | returns presigned S3 URL
                    v
              Phone uploads photo direct to S3 (no Lambda involved)
                    |
                    | POST /receipts/process
                    v
              ProcessorFunction
                    |-- Textract AnalyzeExpense
                    |-- Detect retailer
                    |-- Normalise item names
                    |-- Match categories (exact -> fuzzy -> keyword -> "Other")
                    |-- Save to DynamoDB
                    |
              PWA shows items, user corrects any wrong categories
                    |
                    | POST /receipts/{id}/confirm
                    v
              ReceiptsFunction --> saves corrections to DynamoDB
                                --> updates learning store (mappings table)
                    |
                    | POST /exports
                    v
              ExportFunction --> builds Excel file --> saves to S3
                             --> returns presigned download URL
```

---

## Prerequisites

Install these on Windows before you start.

### 1. Python 3.12
https://www.python.org/downloads/
> Important: tick **"Add Python to PATH"** during install

Verify: `python --version`

### 2. AWS CLI
https://aws.amazon.com/cli/ (download the Windows MSI)

Verify: `aws --version`

Then configure your AWS credentials:
```powershell
aws configure
# AWS Access Key ID: (paste your key)
# AWS Secret Access Key: (paste your secret)
# Default region name: eu-west-2
# Default output format: json
```

### 3. SAM CLI
https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html (download the Windows MSI)

Verify: `sam --version`

### 4. Allow PowerShell scripts (one-time, run as Administrator)
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

---

## First Time Setup

Run these commands in PowerShell from the project folder:

```powershell
# Step 1 - verify everything is installed correctly
.\deploy.ps1 check

# Step 2 - install Python packages
.\deploy.ps1 install

# Step 3 - package the Lambda functions
.\deploy.ps1 build

# Step 4 - create all AWS resources (takes 3-5 minutes)
.\deploy.ps1 deploy

# Step 5 - populate the database with UK categories and retailers
.\deploy.ps1 seed

# Step 6 - get your API URL
.\deploy.ps1 info
```

After step 6 you will see your API Gateway URL. Save it - you will need it for the mobile PWA.

---

## API Endpoints

All requests need the header `X-Api-Key: your-key`.
Get your key from: AWS Console → API Gateway → API Keys.

| Method | Path | What it does |
|---|---|---|
| POST | `/receipts/upload-url` | Get a presigned S3 URL to upload a photo |
| POST | `/receipts/process` | Process an uploaded photo through Textract |
| GET  | `/receipts` | List all receipts |
| GET  | `/receipts/{id}` | Get a receipt with all its items |
| POST | `/receipts/{id}/confirm` | Confirm or correct item categories |
| DELETE | `/receipts/{id}` | Delete a receipt |
| POST | `/exports` | Generate an Excel file and get download URL |
| GET  | `/categories` | List all categories |
| GET  | `/retailers` | List all retailers |
| POST | `/train` | Manually trigger the learning trainer |

---

## How the Category Matching Works

Every item name from the receipt goes through four stages:

```
"TESCO FINEST VINE TOM 500G"
        |
   1. Normalise
      - strip retailer prefix ("TESCO FINEST")
      - lowercase
      - remove weight ("500G")
      - remove punctuation
        |
   "vine tomatoes"
        |
   2. Exact lookup in DynamoDB mappings table
      - if found and confidence >= 0.70 -> use it
        |
   3. Fuzzy match (RapidFuzz) against all known mappings
      - score >= 80 -> use it, save new mapping
        |
   4. Keyword scan against category keyword lists
      - any category keyword appears in the name -> use it
        |
   5. Flag as "needs review" -> user picks the category manually
        |
   User confirms -> saved to mappings table
   After 5 confirmations -> mapping is "trusted" (confidence 0.95)
```

The more receipts you scan and confirm, the better the matching gets.

---

## Supported UK Retailers

Tesco, Sainsbury's, ASDA, Morrisons, Waitrose, Lidl, Aldi, M&S, Co-op, Iceland, Ocado.

Each retailer has a profile in DynamoDB (seeded by `.\deploy.ps1 seed`) that tells the processor:
- What prefixes to strip (e.g. "TESCO FINEST", "TASTE THE DIFFERENCE")
- What lines to skip (e.g. Clubcard points, Nectar points, bag charges)
- How to detect the retailer from the receipt header

---

## Categories

| | Category | Examples |
|---|---|---|
| 🥦 | Fresh Produce | Tomatoes, apples, spinach, mushrooms |
| 🥩 | Meat & Fish | Chicken breast, salmon fillet, bacon |
| 🧀 | Dairy & Eggs | Milk, cheddar, yogurt, eggs |
| 🍞 | Bakery & Bread | Sourdough, croissants, pitta |
| ❄️ | Frozen | Ice cream, frozen peas, fish fingers |
| 🥤 | Drinks | Water, wine, tea, orange juice |
| 🥫 | Cupboard Staples | Pasta, tinned tomatoes, olive oil, spices |
| 🍫 | Snacks & Confectionery | Crisps, chocolate, nuts |
| 🍱 | Ready Meals & Deli | Sandwiches, sushi, hummus |
| 💊 | Health & Beauty | Vitamins, shampoo, toothpaste |
| 🧹 | Household | Washing up liquid, toilet roll, bin bags |
| 👶 | Baby & Child | Nappies, formula, baby food |
| 🐾 | Pet | Cat food, dog treats, litter |
| 📦 | Other | Anything not matched |

---

## Daily Commands

```powershell
# Watch live logs while testing
.\deploy.ps1 logs
.\deploy.ps1 logs -Fn upload
.\deploy.ps1 logs -Fn export

# Reminder of your API URL and resource names
.\deploy.ps1 info

# Re-deploy after making code changes
.\deploy.ps1 build
.\deploy.ps1 deploy
```

---

## Environments

The script supports `dev` and `prod`. Start with dev, move to prod when happy.

```powershell
.\deploy.ps1 deploy -Env dev    # default
.\deploy.ps1 deploy -Env prod   # requires typing 'yes' to confirm
```

---

## Deleting Everything

If you want to start fresh or tear down:

```powershell
# WARNING: deletes the entire AWS stack
.\deploy.ps1 nuke -Env dev
```

Note: S3 buckets that contain files won't be deleted automatically.
Empty them first in the AWS Console (S3 → select bucket → Empty).

---

## Cost Breakdown (~100 receipts/month)

| Service | Monthly cost |
|---|---|
| Textract AnalyzeExpense (100 pages) | ~$1.50 |
| Lambda (500 invocations) | < $0.01 |
| DynamoDB (on-demand, ~5K operations) | < $0.10 |
| S3 (< 100MB storage) | < $0.01 |
| API Gateway (200 requests) | < $0.01 |
| **Total** | **~$1.65/month** |

---

## Project Structure

```
grocery-receipt-scanner/
├── deploy.ps1                       PowerShell script (all commands)
├── infrastructure/
│   ├── template.yaml                SAM CloudFormation template
│   ├── samconfig.toml               Deploy config (dev + prod)
│   └── seed_data.py                 Database seed script
├── lambdas/
│   ├── receipt_upload/              Presigned S3 URL generator
│   ├── receipt_processor/           Textract + category matcher (main logic)
│   ├── receipt_confirm/             Receipt CRUD + user corrections
│   ├── export_generator/            Excel file builder
│   └── category_trainer/            Categories/retailers + learning trainer
├── shared/
│   ├── models.py                    Data models (Pydantic)
│   ├── dynamo_client.py             DynamoDB helpers
│   ├── requirements.txt             Python dependencies
│   └── layer/                       Built by 'install' command
└── tests/
    └── events/                      Test payloads for local testing
```

# =============================================================================
# Grocery Receipt Scanner - PowerShell Script
# =============================================================================
#
# FIRST TIME SETUP - run these in order:
#   .\deploy.ps1 check          <- make sure all tools are installed
#   .\deploy.ps1 install        <- install Python packages
#   .\deploy.ps1 build          <- package the Lambda functions
#   .\deploy.ps1 deploy         <- create everything on AWS
#   .\deploy.ps1 seed           <- populate database with categories/retailers
#   .\deploy.ps1 info           <- print your API URL and resource names
#
# DAY TO DAY:
#   .\deploy.ps1 logs           <- watch Lambda logs live
#   .\deploy.ps1 logs -Fn upload
#   .\deploy.ps1 logs -Fn export
#   .\deploy.ps1 info           <- reminder of your API URL
#
# CLEANUP:
#   .\deploy.ps1 clean          <- delete local build files
#   .\deploy.ps1 nuke           <- DELETE everything from AWS (careful!)
# =============================================================================

param(
    [Parameter(Position=0)]
    [ValidateSet("help","check","install","build","deploy","seed",
                 "info","logs","clean","nuke")]
    [string]$Command = "help",

    # -Env prod  to target production instead of dev
    [ValidateSet("dev","prod")]
    [string]$Env = "dev",

    # -Fn upload|processor|export|reference  to pick which Lambda logs to tail
    [ValidateSet("processor","upload","export","reference")]
    [string]$Fn = "processor",

    # -Profile my-aws-profile  if you use named AWS profiles
    [string]$Profile = "",

    [string]$Region = "eu-west-2"
)

# =============================================================================
# Helpers
# =============================================================================
function Write-Title($text) {
    Write-Host ""
    Write-Host ("=" * 55) -ForegroundColor DarkCyan
    Write-Host "  $text" -ForegroundColor Cyan
    Write-Host ("=" * 55) -ForegroundColor DarkCyan
}
function Write-Step($text)    { Write-Host "`n  >> $text" -ForegroundColor Yellow }
function Write-OK($text)      { Write-Host "  OK  $text" -ForegroundColor Green }
function Write-Warn($text)    { Write-Host "  !!  $text" -ForegroundColor DarkYellow }
function Write-Err($text)     { Write-Host "  ERR $text" -ForegroundColor Red }

function Run($cmd) {
    Write-Host "      $cmd" -ForegroundColor DarkGray
    Invoke-Expression $cmd
    if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) {
        Write-Err "Command failed (exit code $LASTEXITCODE)"
        exit $LASTEXITCODE
    }
}

function AwsArgs {
    $a = "--region $Region"
    if ($Profile) { $a += " --profile $Profile" }
    return $a
}

function StackName { return "grocery-scanner-$Env" }

# =============================================================================
# HELP
# =============================================================================
function Show-Help {
    Write-Title "Grocery Receipt Scanner - Commands"
    Write-Host @"

  FIRST TIME SETUP
  .\deploy.ps1 check              Verify tools (AWS CLI, SAM CLI, Python)
  .\deploy.ps1 install            Install Python dependencies
  .\deploy.ps1 build              Package Lambda functions
  .\deploy.ps1 deploy             Create all AWS resources
  .\deploy.ps1 seed               Seed database with UK categories & retailers

  INFORMATION
  .\deploy.ps1 info               Show your API URL, bucket names, table names

  LOGS
  .\deploy.ps1 logs               Tail receipt processor logs (live)
  .\deploy.ps1 logs -Fn upload    Tail upload Lambda logs
  .\deploy.ps1 logs -Fn export    Tail export Lambda logs

  OPTIONS
  -Env dev|prod                   Target environment (default: dev)
  -Region eu-west-2               AWS region (default: eu-west-2)
  -Profile myprofile              Named AWS CLI profile (optional)

  EXAMPLES
  .\deploy.ps1 deploy -Env prod
  .\deploy.ps1 logs -Fn processor -Env prod
  .\deploy.ps1 deploy -Profile personal-aws

  CLEANUP
  .\deploy.ps1 clean              Delete local build files only
  .\deploy.ps1 nuke -Env dev      Delete AWS stack (asks for confirmation)

"@
}

# =============================================================================
# CHECK PREREQUISITES
# =============================================================================
function Check-Prereqs {
    Write-Title "Checking Prerequisites"
    $ok = $true

    # Python
    Write-Step "Python..."
    $v = python --version 2>&1
    if ($LASTEXITCODE -eq 0) { Write-OK $v }
    else {
        Write-Err "Python not found."
        Write-Host "      Download from: https://www.python.org/downloads/" -ForegroundColor DarkGray
        Write-Host "      Important: tick 'Add Python to PATH' during install" -ForegroundColor DarkGray
        $ok = $false
    }

    # AWS CLI
    Write-Step "AWS CLI..."
    $v = aws --version 2>&1
    if ($LASTEXITCODE -eq 0) { Write-OK $v }
    else {
        Write-Err "AWS CLI not found."
        Write-Host "      Download MSI from: https://aws.amazon.com/cli/" -ForegroundColor DarkGray
        $ok = $false
    }

    # SAM CLI
    Write-Step "SAM CLI..."
    $v = sam --version 2>&1
    if ($LASTEXITCODE -eq 0) { Write-OK $v }
    else {
        Write-Err "SAM CLI not found."
        Write-Host "      Download MSI from: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html" -ForegroundColor DarkGray
        $ok = $false
    }

    # AWS credentials
    Write-Step "AWS credentials..."
    $awsArgs = AwsArgs
    $identity = aws sts get-caller-identity $awsArgs.Split(" ") 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-OK "Credentials valid"
        Write-Host "      $identity" -ForegroundColor DarkGray
    } else {
        Write-Err "AWS credentials not configured."
        Write-Host "      Run: aws configure" -ForegroundColor DarkGray
        Write-Host "      You will need: Access Key ID, Secret Access Key, region (eu-west-2)" -ForegroundColor DarkGray
        $ok = $false
    }

    Write-Host ""
    if ($ok) {
        Write-Host "  Everything looks good! Run: .\deploy.ps1 install" -ForegroundColor Green
    } else {
        Write-Host "  Fix the issues above, then run .\deploy.ps1 check again." -ForegroundColor Red
    }
}

# =============================================================================
# INSTALL
# =============================================================================
function Install-Deps {
    Write-Title "Installing Python Dependencies"

    Write-Step "Creating shared layer folder..."
    New-Item -ItemType Directory -Force -Path "shared\layer\python" | Out-Null

    Write-Step "Installing shared dependencies (boto3, rapidfuzz, openpyxl, pydantic)..."
    Run "pip install -r shared\requirements.txt -t shared\layer\python --quiet"
    Write-OK "Shared layer ready"

    # Individual Lambda requirements (if any)
    foreach ($dir in (Get-ChildItem -Path "lambdas" -Directory)) {
        $req = Join-Path $dir.FullName "requirements.txt"
        if (Test-Path $req) {
            Write-Step "Installing $($dir.Name) dependencies..."
            $vendor = Join-Path $dir.FullName "vendor"
            New-Item -ItemType Directory -Force -Path $vendor | Out-Null
            Run "pip install -r `"$req`" -t `"$vendor`" --quiet"
            Write-OK "$($dir.Name) done"
        }
    }

    Write-Host ""
    Write-OK "All dependencies installed. Next: .\deploy.ps1 build"
}

# =============================================================================
# BUILD
# =============================================================================
function Build-App {
    Write-Title "Building SAM Application"
    Write-Step "Packaging Lambda functions..."

    Push-Location infrastructure
    try {
        Run "sam build"
        Write-Host ""
        Write-OK "Build complete. Next: .\deploy.ps1 deploy"
    } finally {
        Pop-Location
    }
}

# =============================================================================
# DEPLOY
# =============================================================================
function Deploy-App {
    Write-Title "Deploying to AWS ($Env)"

    if ($Env -eq "prod") {
        Write-Host ""
        Write-Warn "You are about to deploy to PRODUCTION."
        $confirm = Read-Host "  Type 'yes' to continue"
        if ($confirm -ne "yes") { Write-Host "  Aborted."; return }
    }

    Write-Step "Deploying stack '$(StackName)' to $Region..."
    Write-Host "  This takes 3-5 minutes. You will see each resource being created." -ForegroundColor DarkGray
    Write-Host ""

    Push-Location infrastructure
    try {
        $extra = if ($Profile) { "--profile $Profile" } else { "" }
        Run "sam deploy --config-env $Env $extra"
    } finally {
        Pop-Location
    }

    Write-Host ""
    Write-OK "Deployment complete!"
    Write-Host ""
    Write-Host "  Next step - seed the database:" -ForegroundColor Cyan
    Write-Host "    .\deploy.ps1 seed -Env $Env" -ForegroundColor White
}

# =============================================================================
# SEED
# =============================================================================
function Seed-DB {
    Write-Title "Seeding Database ($Env)"
    Write-Step "Running seed script..."

    $args = "--env $Env --region $Region"
    if ($Profile) { $args += " --profile $Profile" }
    Run "python infrastructure\seed_data.py $args"

    Write-Host ""
    Write-OK "Database seeded. Your stack is fully ready!"
    Write-Host ""
    Write-Host "  Get your API URL with:" -ForegroundColor Cyan
    Write-Host "    .\deploy.ps1 info -Env $Env" -ForegroundColor White
}

# =============================================================================
# INFO
# =============================================================================
function Show-Info {
    Write-Title "Stack Info: $(StackName)"
    $awsArgs = (AwsArgs).Split(" ")

    $outputs = aws cloudformation describe-stacks `
        --stack-name (StackName) @awsArgs `
        --query "Stacks[0].Outputs" `
        --output json 2>&1

    if ($LASTEXITCODE -ne 0) {
        Write-Err "Stack not found. Have you run .\deploy.ps1 deploy yet?"
        return
    }

    $parsed = $outputs | ConvertFrom-Json
    Write-Host ""
    foreach ($o in $parsed) {
        Write-Host ("  {0,-25} {1}" -f ($o.OutputKey + ":"), $o.OutputValue) -ForegroundColor White
        if ($o.OutputKey -eq "ApiUrl") {
            Write-Host ""
            Write-Host "  Set in PowerShell:" -ForegroundColor DarkGray
            Write-Host "    `$env:API_URL = '$($o.OutputValue)'" -ForegroundColor DarkGray
            Write-Host "  Then test with:" -ForegroundColor DarkGray
            Write-Host "    Invoke-RestMethod -Uri `"`$env:API_URL/categories`" -Headers @{'X-Api-Key'='your-key'}" -ForegroundColor DarkGray
            Write-Host ""
        }
    }
}

# =============================================================================
# LOGS
# =============================================================================
function Tail-Logs {
    $names = @{
        "processor" = "grocery-processor-$Env"
        "upload"    = "grocery-upload-$Env"
        "export"    = "grocery-export-$Env"
        "reference" = "grocery-reference-$Env"
    }
    $logGroup = "/aws/lambda/$($names[$Fn])"
    $awsArgs  = AwsArgs

    Write-Title "Logs: $logGroup"
    Write-Host "  Press Ctrl+C to stop." -ForegroundColor DarkGray
    Write-Host ""
    Run "aws logs tail `"$logGroup`" $awsArgs --follow --format short"
}

# =============================================================================
# CLEAN
# =============================================================================
function Clean-Build {
    Write-Title "Cleaning Build Artifacts"

    foreach ($path in @(".aws-sam", "shared\layer")) {
        if (Test-Path $path) {
            Remove-Item -Recurse -Force $path
            Write-OK "Removed $path"
        }
    }
    Get-ChildItem -Recurse -Filter "__pycache__" -Directory |
        Remove-Item -Recurse -Force
    Get-ChildItem -Path "lambdas" -Recurse -Filter "vendor" -Directory |
        Remove-Item -Recurse -Force
    Write-OK "Cleaned"
}

# =============================================================================
# NUKE (delete AWS stack)
# =============================================================================
function Nuke-Stack {
    Write-Title "DELETE STACK: $(StackName)"
    Write-Host ""
    Write-Warn "This will permanently delete the CloudFormation stack and all resources."
    Write-Warn "S3 buckets that contain files will NOT be deleted - empty them first in the AWS Console."
    Write-Host ""
    $confirm = Read-Host "  Type the stack name '$(StackName)' to confirm"
    if ($confirm -ne (StackName)) {
        Write-Host "  Aborted - name didn't match." -ForegroundColor Yellow
        return
    }

    $awsArgs = AwsArgs
    Run "aws cloudformation delete-stack --stack-name $(StackName) $awsArgs"
    Write-Host ""
    Write-OK "Deletion started. Monitor in AWS Console: CloudFormation -> Stacks"
    Write-Host "  It takes 2-3 minutes to finish." -ForegroundColor DarkGray
}

# =============================================================================
# ROUTER
# =============================================================================
Write-Host ""
switch ($Command) {
    "help"    { Show-Help    }
    "check"   { Check-Prereqs}
    "install" { Install-Deps }
    "build"   { Build-App    }
    "deploy"  { Deploy-App   }
    "seed"    { Seed-DB      }
    "info"    { Show-Info    }
    "logs"    { Tail-Logs    }
    "clean"   { Clean-Build  }
    "nuke"    { Nuke-Stack   }
    default   { Show-Help    }
}

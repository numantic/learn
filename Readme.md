# --- CONFIGURATION ---
$adfName = "YOUR_ADF_NAME"
$amlWorkspaceName = "YOUR_AML_WORKSPACE_NAME"
$resourceGroupName = "YOUR_RESOURCE_GROUP"

# 1. Get ADF Managed Identity Details
$adf = Get-AzDataFactoryV2 -ResourceGroupName $resourceGroupName -Name $adfName
$adfPrincipalId = $adf.Identity.PrincipalId
Write-Host "Checking ADF Identity: $($adf.Name) (ID: $adfPrincipalId)" -ForegroundColor Cyan

# 2. Check RBAC: ADF -> AML Workspace
$amlWorkspace = Get-AzResource -ResourceGroupName $resourceGroupName -ResourceType "Microsoft.MachineLearningServices/workspaces" -Name $amlWorkspaceName
$amlScope = $amlWorkspace.ResourceId

Write-Host "`n[1/3] Checking if ADF has 'Data Scientist' role on AML Workspace..." -ForegroundColor Yellow
$roles = Get-AzRoleAssignment -ObjectId $adfPrincipalId -Scope $amlScope
if ($roles.RoleDefinitionName -contains "AzureML Data Scientist" -or $roles.RoleDefinitionName -contains "Contributor") {
    Write-Host "[OK] ADF has sufficient permissions on AML Workspace." -ForegroundColor Green
} else {
    Write-Host "[FAIL] ADF missing 'AzureML Data Scientist' role on the Workspace." -ForegroundColor Red
}

# 3. Check RBAC: AML Workspace Identity -> Storage and ACR
# AML needs to be able to pull images and read data
$amlIdentityId = (Get-AzResource -Id $amlScope -ExpandProperties).Identity.PrincipalId
$storageId = (Get-AzResource -Id $amlScope -ExpandProperties).Properties.storageAccount
$acrId = (Get-AzResource -Id $amlScope -ExpandProperties).Properties.containerRegistry

Write-Host "`n[2/3] Checking AML Workspace's own permissions..." -ForegroundColor Yellow

# Check Storage
$storageRoles = Get-AzRoleAssignment -ObjectId $amlIdentityId -Scope $storageId
if ($storageRoles.RoleDefinitionName -match "Storage Blob Data") {
    Write-Host "[OK] AML Workspace can access Storage." -ForegroundColor Green
} else {
    Write-Host "[FAIL] AML Identity missing 'Storage Blob Data Contributor' on Storage Account." -ForegroundColor Red
}

# Check ACR
$acrRoles = Get-AzRoleAssignment -ObjectId $amlIdentityId -Scope $acrId
if ($acrRoles.RoleDefinitionName -contains "AcrPull") {
    Write-Host "[OK] AML Workspace can pull images from ACR." -ForegroundColor Green
} else {
    Write-Host "[FAIL] AML Identity missing 'AcrPull' on Container Registry." -ForegroundColor Red
}

# 4. Check Networking
Write-Host "`n[3/3] Checking Network Isolation..." -ForegroundColor Yellow
$workspaceProperties = Get-AzResource -Id $amlScope -ExpandProperties
$publicAccess = $workspaceProperties.Properties.publicNetworkAccess

if ($publicAccess -eq "Disabled") {
    Write-Host "[WARN] Public Network Access is DISABLED. Ensure ADF is using a Managed VNet Integration Runtime with a Private Endpoint to AML." -ForegroundColor DarkYellow
} else {
    Write-Host "[OK] Public Network Access is ENABLED." -ForegroundColor Green
}

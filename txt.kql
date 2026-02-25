$kqlQuery = @"
resources
| where type =~ 'microsoft.batch/batchaccounts' or type =~ 'microsoft.storage/storageaccounts'
| project
    Name = name,
    Type = type,
    ResourceGroup = resourceGroup,
    SubscriptionId = subscriptionId,
    Location = location,
    PublicAccess = properties.publicNetworkAccess,
    IdentityType = iif(isnotnull(identity.type), identity.type, "None"),
    HttpsOnly = properties.supportsHttpsTrafficOnly,
    MinTls = properties.minimumTlsVersion,
    PoolAllocationMode = properties.poolAllocationMode,
    ResourceId = id
| join kind=leftouter (
    authorizationresources
    | where type =~ 'microsoft.authorization/roleassignments'
    | project
        RoleAssignmentId = name,
        PrincipalId = properties.principalId,
        PrincipalType = properties.principalType,
        RoleDefinitionId = properties.roleDefinitionId,
        ResourceId = tolower(tostring(properties.scope))
    | join kind=leftouter (
        authorizationresources
        | where type =~ 'microsoft.authorization/roledefinitions'
        | project
            RoleDefinitionId = id,
            RoleName = properties.roleName,
            RoleType = properties.type,
            RoleDescription = properties.description
    ) on RoleDefinitionId
) on ResourceId
| project
    Name, Type, ResourceGroup, SubscriptionId, Location,
    PublicAccess, IdentityType, HttpsOnly, MinTls, PoolAllocationMode,
    RoleName, RoleType, PrincipalId, PrincipalType, RoleAssignmentId, Scope
| order by Type asc, Name asc
"@

$graphResults = Search-AzGraph -Query $kqlQuery -First 1000
$graphResults | Format-Table -AutoSize
# $graphResults | Export-Csv -Path $filePath -NoTypeInformation

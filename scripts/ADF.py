from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    PipelineResource,
    CopyActivity,
    BlobSource,
    BlobSink,
    DatasetReference,
    LinkedServiceReference,
    DatasetResource,
    AzureBlobStorageLinkedService,
    AzureBlobDataset,
    MappingDataFlow,
    DataFlowSource,
    DerivedColumnTransformation,
    DataFlowSink,
    DataFlow,
)

# Azure Details
subscription_id = "<your-subscription-id>"
resource_group_name = "charity-group"
data_factory_name = "CharityDataFactory"
storage_account_name = "charitydatastorage"
raw_container = "raw"
transformed_container = "transformed"


credential = DefaultAzureCredential()
adf_client = DataFactoryManagementClient(credential, subscription_id)


storage_linked_service = AzureBlobStorageLinkedService(
    connection_string="DefaultEndpointsProtocol=https;AccountName=charitydatastorage;AccountKey=<your-key>;EndpointSuffix=core.windows.net"
)

adf_client.linked_services.create_or_update(
    resource_group_name, data_factory_name, "AzureBlobStorage", storage_linked_service
)


raw_dataset = DatasetResource(
    properties=AzureBlobDataset(
        linked_service_name=LinkedServiceReference(reference_name="AzureBlobStorage"),
        folder_path=raw_container,
        file_name="*.csv",
    )
)

transformed_dataset = DatasetResource(
    properties=AzureBlobDataset(
        linked_service_name=LinkedServiceReference(reference_name="AzureBlobStorage"),
        folder_path=transformed_container,
        file_name="*.csv",
    )
)

adf_client.datasets.create_or_update(
    resource_group_name, data_factory_name, "RawDataset", raw_dataset
)

adf_client.datasets.create_or_update(
    resource_group_name, data_factory_name, "TransformedDataset", transformed_dataset
)


copy_activity = CopyActivity(
    name="CopyRawToTransformed",
    source=BlobSource(),
    sink=BlobSink(),
    inputs=[DatasetReference(reference_name="RawDataset")],
    outputs=[DatasetReference(reference_name="TransformedDataset")],
)


pipeline = PipelineResource(activities=[copy_activity])

adf_client.pipelines.create_or_update(
    resource_group_name, data_factory_name, "TransformAndCopyPipeline", pipeline
)

print("Pipeline created successfully!")

using System;
using System.Activities;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Workflow;

namespace DS.DynamicsCRM.WARollup
{
    public class Activity : CodeActivity
    {
        [RequiredArgument]
        [Input("Child Entity Field to Roll-Up (example: baseamount)")]
        public InArgument<string> ChildRollupFieldName { get; set; }

        [RequiredArgument]
        [Input("Child Entity Lookup Field (example: salesorderid)")]
        public InArgument<string> ChildLookupFieldName { get; set; }

        [RequiredArgument]
        [Input("Parent Entity Name (example: salesorder)")]
        public InArgument<string> ParentEntityName { get; set; }

        [RequiredArgument]
        [Input("Parent Entity Roll-Up Result Field (example: totalamount)")]
        public InArgument<string> ParentRollupResultFieldName { get; set; }

        [Default("true")]
        [RequiredArgument]
        [Input("Debug Mode (Turn on to get error messages)")]
        public InArgument<bool> DebugMode { get; set; }

        private bool debugMode;
        private CodeActivityContext context;
        private IWorkflowContext workflowContext;
        private IOrganizationServiceFactory serviceFactory;
        private IOrganizationService service;

        protected override void Execute(CodeActivityContext _context)
        {
            context = _context;
            debugMode = DebugMode.Get(context);

            workflowContext = context.GetExtension<IWorkflowContext>();
            serviceFactory = context.GetExtension<IOrganizationServiceFactory>();
            service = serviceFactory.CreateOrganizationService(workflowContext.InitiatingUserId);

            // Checking Depth is considered a bad habit since it should not be necessary in a well designed
            // environment. However, it's done here since we don't know the environment we are executing in.
            if (!debugMode && workflowContext.Depth > 1)
                return;

            var childLookupFieldName = ChildLookupFieldName.Get(context);
            var childRollupFieldName = ChildRollupFieldName.Get(context);
            var parentEntityName = ParentEntityName.Get(context);
            var parentRollupResultFieldName = ParentRollupResultFieldName.Get(context);

            var parentEntityId = GetParentEntityId(childLookupFieldName);
            var rollupResult = GetRollupResult(parentEntityId, childRollupFieldName, childLookupFieldName);
            UpdateRollupResultOnParent(parentEntityName, parentEntityId, parentRollupResultFieldName, rollupResult);
        }

        private Guid GetParentEntityId(string childLookupFieldName)
        {
            Guid parentEntityId = Guid.Empty;

            try
            {
                var columnSet = new ColumnSet(new string[] { childLookupFieldName });
                var childEntity = service.Retrieve(
                    workflowContext.PrimaryEntityName,
                    workflowContext.PrimaryEntityId,
                    columnSet
                );

                parentEntityId = ((EntityReference)childEntity[childLookupFieldName]).Id;
            }
            catch (Exception e)
            {
                if (debugMode)
                    throw new InvalidPluginExecutionException("Something went wrong. Make sure that the Child Lookup Field ('" + childLookupFieldName + "') is correct.");
            }

            return parentEntityId;
        }

        private Money GetRollupResult(Guid parentEntityId, string childRollupFieldName, string childLookupFieldName)
        {
            Money rollupResult = null;

            var xml = @"<fetch distinct='true' aggregate='true' >
                          <entity name='" + workflowContext.PrimaryEntityName + @"' >
                            <attribute name='" + childRollupFieldName + @"' alias='ds_rolluptotal' aggregate='sum' />
                            <filter>
                              <condition attribute='" + childLookupFieldName + @"' operator='eq' value='" + parentEntityId + @"' />
                              <condition attribute='statecode' operator='eq' value='0' />
                            </filter>
                          </entity>
                        </fetch>";

            var result = service.RetrieveMultiple(new FetchExpression(xml));
            if (result.Entities.Count > 0 && result.Entities[0].Contains("ds_rolluptotal"))
            {
                var aliasedValue = (AliasedValue)result.Entities[0]["ds_rolluptotal"];
                rollupResult = (Money)aliasedValue.Value;
            }

            return rollupResult;
        }

        private void UpdateRollupResultOnParent(string parentEntityName, Guid parentEntityId, string parentRollupResultFieldName, Money rollupResult)
        {
            var updateParentEntity = new Entity(parentEntityName, parentEntityId);
            updateParentEntity.Attributes.Add(parentRollupResultFieldName, rollupResult);

            try
            {
                service.Update(updateParentEntity);
            }
            catch (Exception e)
            {
                if (debugMode)
                    throw new InvalidPluginExecutionException("Something went wrong. Make sure that the Parent Entity Name ('" + parentEntityName + "' and Parent Entity Roll-up Result Field ('" + parentRollupResultFieldName + "') are correct.");
                else
                    return;
            }
        }
    }
}

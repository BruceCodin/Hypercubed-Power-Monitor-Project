"""
Simple orchestrator for 30-minute ETL pipeline.
Calls existing lambda_handler_carbon and lambda_handler_elexon.
"""
import logging
import sys
import os

# Add pipeline directories to Python path (for local testing)
# In Lambda, all files are at root so this has no effect
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'carbon_pipeline'))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'elexon_pipeline'))

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Orchestrates both Carbon and Elexon pipelines.
    Runs them sequentially and reports combined results.
    """
    try:
        logger.info("=" * 60)
        logger.info("STARTING 30-MINUTE ETL PIPELINE")
        logger.info("=" * 60)
        
        # Import both handlers
        from carbon_pipeline.lambda_handler_carbon import lambda_handler as carbon_handler
        from elexon_pipeline.lambda_handler_elexon import lambda_handler as elexon_handler
        
        # Run Carbon pipeline
        logger.info("\n[1/2] Running Carbon Intensity pipeline...")
        carbon_result = carbon_handler(event, context)
        carbon_success = carbon_result['statusCode'] == 200
        logger.info("Carbon result: %s", carbon_result['body'])
        
        # Run Elexon pipeline
        logger.info("\n[2/2] Running Elexon pipeline...")
        elexon_result = elexon_handler(event, context)
        elexon_success = elexon_result['statusCode'] == 200
        logger.info("Elexon result: %s", elexon_result['body'])
        
        # Summary
        logger.info("=" * 60)
        logger.info("PIPELINE SUMMARY")
        logger.info("Carbon:  %s", '✓ SUCCESS' if carbon_success else '✗ FAILED')
        logger.info("Elexon:  %s", '✓ SUCCESS' if elexon_success else '✗ FAILED')
        logger.info("=" * 60)
        
        # Return combined result
        if carbon_success and elexon_success:
            return {
                'statusCode': 200,
                'body': 'All pipelines completed successfully'
            }
        elif carbon_success or elexon_success:
            return {
                'statusCode': 200,
                'body': f'Partial success - Carbon: {carbon_success}, Elexon: {elexon_success}'
            }
        else:
            return {
                'statusCode': 500,
                'body': 'All pipelines failed'
            }
        
    except Exception as e:
        logger.error("Critical error in orchestrator: %s", e, exc_info=True)
        return {
            'statusCode': 500,
            'body': f'Orchestrator failed: {str(e)}'
        }


if __name__ == "__main__":
    """Allow running the orchestrator locally for testing"""
    print("Running 30-Minute ETL Pipeline locally...")
    print("=" * 60)
    
    # Mock Lambda event and context
    mock_event = {}
    mock_context = type('Context', (), {
        'function_name': 'test-30min-pipeline',
        'aws_request_id': 'local-test'
    })()
    
    # Run the orchestrator
    result = lambda_handler(mock_event, mock_context)
    
    print("=" * 60)
    print(f"Final Status: {result['statusCode']}")
    print(f"Final Body: {result['body']}")
    print("=" * 60)
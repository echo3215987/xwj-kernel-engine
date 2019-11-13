//import com.foxconn.iisd.bd.rca.ValidationData
//import com.foxconn.iisd.bd.rca.utils.SummaryFile
//import org.scalatest.{FeatureSpec, GivenWhenThen}
//
//class ValidationDataTest extends FeatureSpec with GivenWhenThen with Matchers {
//
//  info("validate master , detail and test file data")
//
//  feature("validate empty data") {
//
//    scenario("if master , detail and test file is empty file") {
//      Given("empty data count is zero")
//      val dataCount = 0
//
//      When("master , detail and test data in summaryfile is empty")
////      SummaryFile.masterFilesTotalRowsDist = dataCount
////      SummaryFile.detailFilesTotalRowsDist = dataCount
////      SummaryFile.testFilesTotalRowsDist = dataCount
//
//      Then("ValidationData.validateEmptyData is true")
////      ValidationData.validateEmptyData() should be(true)
//    }
//
//    scenario("validate not empty data") {
//      Given("data count is one")
//      val dataCount = 1
//
//      When("master , detail and test data count in summaryfile is one")
////      SummaryFile.masterFilesTotalRowsDist = dataCount
////      SummaryFile.detailFilesTotalRowsDist = dataCount
////      SummaryFile.testFilesTotalRowsDist = dataCount
//
//      Then("ValidationData.validateEmptyData is false")
////      ValidationData.validateEmptyData() should be(false)
//    }
//
//  }
//
//}
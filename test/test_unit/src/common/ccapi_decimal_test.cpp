#include "gtest/gtest.h"

#include "ccapi_cpp/ccapi_decimal.h"
namespace ccapi {
TEST(DecimalTest, compareScientificNotation) {
  Decimal bid_1("1.51e-6");
  Decimal ask_1("1.5113e-6");
  EXPECT_LT(bid_1, ask_1);
  Decimal bid_2("1.51E-6");
  Decimal ask_2("1.5113E-6");
  EXPECT_LT(bid_2, ask_2);
}
TEST(DecimalTest, scientificNotation) {
  {
    Decimal x("1.51e-6");
    EXPECT_EQ(x.toString(), "0.00000151");
  }
{
  Decimal x("1.51E-6");
  EXPECT_EQ(x.toString(), "0.00000151");
}
{
  Decimal x("3.14159e+000");
  EXPECT_EQ(x.toString(), "3.14159");
}
{
  Decimal x("2.00600e+003");
  EXPECT_EQ(x.toString(), "2006");
}
{
  Decimal x("1.00000e-010");
  EXPECT_EQ(x.toString(), "0.0000000001");
}
}
TEST(DecimalTest, trailingZero) {
  Decimal bid_1("0.10");
  EXPECT_EQ(bid_1.toString(), "0.1");
}
TEST(DecimalTest, subtract) { EXPECT_EQ(Decimal("0.000000549410817836").subtract(Decimal("0")).toString(), "0.000000549410817836"); }
} /* namespace ccapi */

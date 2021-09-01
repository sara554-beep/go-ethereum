 pragma solidity ^0.8.0;

 contract Test {
     struct MyStruct{
         uint256 a;
         uint256 b;
     }
     event StructEvent(MyStruct s);
     function TestEvent() public {
         emit StructEvent(MyStruct({a: 1, b: 2}));
     }
 }
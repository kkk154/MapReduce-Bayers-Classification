package utils;

public class Tools {

    //判断一个字符串是否为数值
    public static boolean isFloat(String str){
        try{
            Float.parseFloat(str);
            return true;
        }
        catch(NumberFormatException e) {
            return false;
        }
    }

    public static void main(String args[])
    {
        String[] num1 = {
                "123"    ,"12.3","0.0","-100","+100","-100.0","+100.0","+0.0","-0.0","1231321.321321"
        };
        for (String s:num1) {
            if(!isFloat(s))
            {
                System.out.println("字符串"+s+"不是"+"数值");
                continue;
            }
            System.out.println("字符串"+s+"是"+"数值");
        }
    }
}

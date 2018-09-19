package com.nypost.Contants;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class jood {

       public static void lexicalOrder(int[] arr) {

           int max_so_far =0;
           int max_end_here =0;

           for(int i=0;i<arr.length;i++){
               max_end_here += arr[i];
               if(max_end_here<0) max_end_here=0;
               if(max_so_far<max_end_here) max_so_far=max_end_here;
           }
           System.out.println(max_so_far);
       }

    public static void main(String[] args) {
         int arr[] = {-2,-3,4,-1,-2,1,5,-3};
        lexicalOrder(arr);
    }
}
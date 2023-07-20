Follow the instructions to run successfully the solution:


1. Start by locating the "DockerDir" folder, which contains two subfolders: "TwoDatabasesDDP" and "TwoDatabasesDDP2". Each folder corresponds to a different part of the      assignment and has its own set of Java files.

2. Open a terminal and navigate to the directory where you have the "DockerDir" folder.

3. Change directory to "TwoDatabasesDDP" by typing the following command in the terminal: cd TwoDatabasesDDP

4. To verify that you are in the correct folder and can see the files inside it, use the command: ls

5. Build the first image named "ddp-node" by running the following command: docker build -t ddp-node .

6. Now, you can run the image and create a container with two volumes that will hold the two empty databases. Execute the command: 
docker run -v database1_volume:/volumes/database1 -v database2_volume:/volumes/database2 ddp-node

7. Wait until you see the message "Program was run successfully!" in the console.

8. At this point, the two databases have been populated with data, and you can proceed to build and run the second image that inherits the volumes from the first image.

9. Navigate to the "TwoDatabasesDDP2" folder by typing: cd ../TwoDatabasesDDP2

10. To ensure that you are in the correct folder and can see the files inside it, use the command: ls

11. Build the second image named "ddp-manipulate-node" by running the following command: docker build -t ddp-manipulate-node .

12. Now, you can run the image and create a container with the two volumes that contain the filled databases. Execute the command:
docker run -it -v database1_volume:/volumes/database1 -v database2_volume:/volumes/database2 ddp-manipulate-node

13. In the console, you will be able to see the results of the program execution.



By following these steps, you should be able to successfully run the solution.



# purplerock vault

I use a zettlekasten-style markdown editor, a purplerock editor. At the moment that's Obsidian. But markdown is markdown.

I want to have different views of my vault from a number of devices. I achieve this by syncing every machiens vaault to a separate directory with syncthing and then using the tools here to merge the changes.

purplerock-vault keeps track of versions markdown files at the change and allows merging data between different vaults in a lot of ways.

This is AI-generated and unreviewed and I have not used it. I suggest reviewing the code and testing before using it in production.


## Why not git?
There are tools to use Obsidian with vault.
It will break and then be an utter pain to fix that is hard to understand.


## Versioning
Files can diverge. The way we deal with this is keeping track of versions of a file and their parents automatically. If one machine changes one file bit by bit to another version. We know that we can replay those chnges on another machine.

## Merging
This task of copying files and changers from one tree to another.

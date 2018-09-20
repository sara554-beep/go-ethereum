pragma solidity ^0.4.24;

/**
 * @title Registrar
 * @author Gary Rong<garyrong0905@gmail.com>
 * @dev Implementation of the blockchain checkpoint registrar.
 */
contract Registrar {
    /*
        Modifiers
    */

    /**
     * @dev Check whether the message sender is authorized.
     */
    modifier OnlyAuthorized() {
        require(admins[msg.sender] > 0);
        _;
    }

    /*
        Events
    */

    // NewCheckpointEvent is emitted when new checkpoint is registered or modified.
    // We use checkpoint hash instead of the full checkpoint to make the transaction cheaper.
    event NewCheckpointEvent(uint indexed index, bytes32 checkpointHash, bytes signature);

    /*
        Public Functions
    */
    constructor(address[] _adminlist, uint _sectionSize, uint _processConfirms, uint _freezeThreshold) public {
        for (uint i = 0; i < _adminlist.length; i++) {
            admins[_adminlist[i]] = 1;
            adminList.push(_adminlist[i]);
        }
        sectionSize = _sectionSize;
        processConfirms = _processConfirms;
        freezeThreshold = _freezeThreshold;
    }

    /**
     * @dev Get latest stable checkpoint information.
     * @return section index
     * @return checkpoint hash
     */
    function GetLatestCheckpoint()
    view
    public
    returns(uint, bytes32) {
        bytes32 hash = GetCheckpoint(latest);
        return (latest, hash);
    }

    /**
     * @dev Get a stable checkpoint information with specified section index.
     * @param _sectionIndex section index
     * @return checkpoint hash
     */
    function GetCheckpoint(uint _sectionIndex)
    view
    public
    returns(bytes32)
    {
        return checkpoints[_sectionIndex];
    }

    /**
     * @dev Set stable checkpoint information.
     * Checkpoint represents a set of post-processed trie roots (CHT and BloomTrie)
     * associated with the appropriate section head hash.
     *
     * It is used to start light syncing from this checkpoint
     * and avoid downloading the entire header chain while still being able to securely
     * access old headers/logs.
     *
     * Note we trust the given information here provided by foundation,
     * need a trust less version for future.
     * @param _sectionIndex section index
     * @param _hash checkpoint hash calculated in the client side
     * @param _sig admin's signature for checkpoint hash and index
     * @return indicator whether set checkpoint successfully
     */
    function SetCheckpoint(
        uint _sectionIndex,
        bytes32 _hash,
        bytes _sig
    )
    OnlyAuthorized
    public
    returns(bool)
    {
        // Ensure the checkpoint information provided is strictly continuous with previous one.
        // But the latest checkpoint modification is allowed.
        if (_sectionIndex != latest && _sectionIndex != latest + 1 && latest != 0) {
            return false;
        }
        // Ensure the checkpoint is stable enough to be registered.
        if (block.number < (_sectionIndex+1)*sectionSize+processConfirms) {
            return false;
        }
        // Ensure the modification for registered checkpoint within the allowed time range
        if (latest != 0 && _sectionIndex == latest && block.number >= (_sectionIndex+1)*sectionSize+freezeThreshold) {
            return false; 
        }

        checkpoints[_sectionIndex] = _hash;
        latest = _sectionIndex;

        emit NewCheckpointEvent(_sectionIndex, _hash, _sig);
    }

    /**
     * @dev Get all admin addresses
     * @return address list
     */
    function GetAllAdmin()
    public
    view
    returns(address[])
    {
        address[] memory ret = new address[](adminList.length);
        for (uint i = 0; i < adminList.length; i++) {
            ret[i] = adminList[i];
        }
        return ret;
    }

    /*
        Fields
    */

    // A map of admin users who have the permission to update CHT and bloom Trie root
    mapping(address => uint) admins;

    // A list of admin users so that we can obtain all admin users.
    address[] adminList;

    // Registered checkpoint information
    mapping(uint => bytes32) checkpoints;

    // Latest stored section id
    // Note all registered checkpoint information should continuous with previous one.
    uint latest;

    // The frequency for creating a checkpoint
    //
    // The default value should be the same as the checkpoint size(32768) in the ethereum.
    uint sectionSize;

    // The number of confirmations needed before a checkpoint can be registered.
    // We have to make sure the checkpoint registered will not be invalid due to
    // chain reorg.
    //
    // The default value should be the same as the checkpoint process confirmations(256) 
    // in the ethereum.
    uint processConfirms;
    
    // The maximum time range in which a registered checkpoint is allowed to be modified.
    //
    // The default value should be the same as the checkpoint accept confirmation(8192) 
    // in the ethereum after which a local generated checkpoint can finally be converted 
    // to a stable one in the server side.
    uint freezeThreshold;
}


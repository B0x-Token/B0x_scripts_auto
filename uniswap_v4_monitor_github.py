def batch_validate_positions(self, position_data: List[Tuple[int, str, int]], batch_size: int = 20) -> Tuple[List[Position], List[Position]]:
        """
        Validate positions in batches to avoid overwhelming the RPC endpoint
        """
        all_valid = []
        all_invalid = []
        
        # Process in batches
        for i in range(0, len(position_data), batch_size):
            batch = position_data[i:i + batch_size]
            print(f"Processing validation batch {i//batch_size + 1}/{(len(position_data) + batch_size - 1)//batch_size}")
            
            valid, invalid = self.validate_positions(batch)
            all_valid.extend(valid)
            all_invalid.extend(invalid)
            
            # Delay between batches to be nice to RPC
            if i + batch_size < len(position_data):
                print("Pausing between validation batches...")
                time.sleep(3)
        
        return all_valid, all_invalid

    def calculate_block_range(self, from_block: int, to_block: int) -> List[Tuple[int, int]]:
        """Calculate block ranges to stay within log and block limits"""
        ranges = []
        current = from_block
        
        while current <= to_block:
            # Use the smaller of max_blocks_per_request or remaining blocks
            end_block = min(current + self.max_blocks_per_request - 1, to_block)
            ranges.append((current, end_block))
            current = end_block + 1
            
        return ranges

    def get_newly_minted_positions(self, transfer_logs):
        """Extract token IDs that were minted (transferred from zero address)"""
        newly_minted = []
        zero_address = "0x0000000000000000000000000000000000000000000000000000000000000000"
        
        for log in transfer_logs:
            # Transfer event structure: Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
            # topics[0] = event signature
            # topics[1] = from address
            # topics[2] = to address
            # topics[3] = tokenId
            
            if len(log['topics']) >= 4:
                from_address = log['topics'][1]
                token_id = int(log['topics'][3], 16)  # Convert hex to int
                print(f"From Address: {from_address}")
                # Check if transfer is from zero address (minting)
                if from_address.lower() == zero_address.lower():
                    newly_minted.append(token_id)
                    print(f"  Found newly minted token: {token_id}")
        
        return newly_minted

    def scan_blocks(self, from_block: int, to_block: int):
        """Scan a range of blocks for events"""
        print(f"\nScanning blocks {from_block} to {to_block}...")
        
        # Split into smaller ranges if needed
        block_ranges = self.calculate_block_range(from_block, to_block)
        
        all_new_positions = []
        all_transfers = {}
        
        for start, end in block_ranges:
            print(f"  Scanning sub-range: {start} to {end} ({end - start + 1} blocks)")
            
            # Get Transfer logs with retry
            transfer_logs = self.get_logs(
                from_block=start,
                to_block=end,
                topics=[self.transfer_topic],
                address=self.nft_address
            )
            
            print(f"    Found {len(transfer_logs)} Transfer events")
            
            if transfer_logs:
                # Process all transfers
                transfers = self.process_transfer_logs(transfer_logs)
                all_transfers.update(transfers)
                
                # Identify new position creations (transfers from zero address)
                new_position_token_ids = self.get_newly_minted_positions(transfer_logs)
                
                if new_position_token_ids:
                    print(f"    Found {len(new_position_token_ids)} newly minted positions")
                    
                    # Validate new positions using the MultiCall implementation
                    # Convert token_ids to position_data format for validate_positions
                    position_data = []
                    for token_id in new_position_token_ids:
                        # Extract tx_hash from the transfer logs for this token_id
                        tx_hash = ""
                        for log in transfer_logs:
                            if (len(log.get('topics', [])) >= 4 and 
                                int(log['topics'][3], 16) == token_id and
                                log['topics'][1].lower() == "0x0000000000000000000000000000000000000000000000000000000000000000"):
                                tx_hash = log.get('transactionHash', '')
                                break
                        
                        position_data.append((token_id, tx_hash, end))
                    
                    # Validate positions using MultiCall
                    valid, invalid = self.validate_positions(position_data)
                    
                    # Add to our collections
                    self.valid_positions.extend(valid)
                    self.invalid_positions.extend(invalid)
                    
                    # Track these as new positions found
                    all_new_positions.extend(new_position_token_ids)
                    
                    print(f"    Validation results: {len(valid)} valid, {len(invalid)} invalid positions")
        
        # Update NFT ownership for all transfers
        if all_transfers:
            print(f"  Updating NFT ownership for {len(all_transfers)} transfers")
            self.nft_owners.update(all_transfers)
            
            # Update ownership info in valid positions
            updated_count = 0
            for position in self.valid_positions:
                if position.token_id in all_transfers:
                    old_owner = position.owner
                    position.owner = all_transfers[position.token_id]
                    if old_owner != position.owner:
                        updated_count += 1
                        print(f"    Updated owner for token {position.token_id}: {old_owner} -> {position.owner}")
            
            if updated_count > 0:
                print(f"  Updated ownership for {updated_count} valid positions")
        
        # Summary for this block range
        if all_new_positions:
            print(f"  Block range summary: {len(all_new_positions)} new positions discovered")
        
        return all_new_positions

    def _get_latest_block_internal(self) -> int:
        """Internal method for getting latest block (without retry logic)"""
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 1
        }
        
        self.rate_limit_pause()
        response = requests.post(self.rpc_url, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        
        if "error" in result:
            error_msg = result['error']
            if isinstance(error_msg, dict):
                error_msg = error_msg.get('message', str(error_msg))
            raise Exception(f"RPC Error: {error_msg}")
            
        return int(result.get("result", "0x0"), 16)

    def get_latest_block(self) -> int:
        """Get the latest block number with retry logic"""
        try:
            return self.retry_with_backoff(self._get_latest_block_internal)
        except Exception as e:
            print(f"Failed to get latest block after {self.max_retries} attempts: {e}")
            return self.current_block

    def print_summary(self):
        """Print a summary of findings"""
        print(f"\n{'='*50}")
        print("SUMMARY")
        print(f"{'='*50}")
        print(f"Blocks scanned: {self.start_block} to {self.current_block}")
        print(f"Valid positions: {len(self.valid_positions)}")
        print(f"Invalid positions: {len(self.invalid_positions)}")
        print(f"NFT owners tracked: {len(self.nft_owners)}")
        
        if self.valid_positions:
            print(f"\nVALID POSITIONS:")
            for pos in self.valid_positions:
                print(f"  Token ID: {pos.token_id}, Owner: {pos.owner}, Block: {pos.block_number}")
        else:
            print(f"\nNo valid positions found yet")
        
        if self.nft_owners:
            print(f"\nVALID NFT OWNERS (tracked):")
            for token_id, owner in self.nft_owners.items():
                print(f"  Token ID: {token_id}, Owner: {owner}")
        else:
            print(f"No valid NFT owners being tracked")

    def run_once(self, blocks_per_scan: int = 1000):
        """Run a single scan cycle to the latest block"""
        latest_block = self.get_latest_block()
        
        if self.current_block <= latest_block:
            # Calculate how many blocks to scan (don't exceed blocks_per_scan in one go)
            blocks_to_scan = min(blocks_per_scan, latest_block - self.current_block + 1)
            to_block = self.current_block + blocks_to_scan - 1
            
            print(f"Latest block: {latest_block}, Current: {self.current_block}, Scanning to: {to_block}")
            
            self.scan_blocks(self.current_block, to_block)
            self.current_block = to_block + 1
            
            # Auto-save after each scan
            self.save_data()
        else:
            print(f"Already caught up to block {latest_block}")
        
        self.print_summary()
        return latest_block

    def run_continuous(self, blocks_per_scan: int = 1000, sleep_seconds: int = 10):
        """Run continuous monitoring - always scan to the newest block"""
        print("Starting continuous monitoring...")
        print("Will continuously scan to the newest block")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                latest_block = self.get_latest_block()
                
                if self.current_block <= latest_block:
                    # There are new blocks to scan
                    remaining_blocks = latest_block - self.current_block + 1
                    print(f"\n{remaining_blocks} blocks behind latest ({self.current_block} → {latest_block})")
                    
                    # Scan in chunks if there are many blocks to catch up
                    while self.current_block <= latest_block:
                        # Re-get latest block in case it changed during scanning
                        current_latest = self.get_latest_block()
                        blocks_to_scan = min(blocks_per_scan, current_latest - self.current_block + 1)
                        to_block = self.current_block + blocks_to_scan - 1
                        
                        if blocks_to_scan > 0:
                            print(f"Scanning blocks {self.current_block} to {to_block} ({blocks_to_scan} blocks)")
                            self.scan_blocks(self.current_block, to_block)
                            self.current_block = to_block + 1
                            
                            # Auto-save after each chunk
                            self.save_data()
                            
                            # Brief pause between chunks to avoid overwhelming RPC
                            if self.current_block <= current_latest:
                                time.sleep(2)
                        else:
                            break
                    
                    self.print_summary()
                    print(f"✓ Caught up to block {latest_block}")
                else:
                    print(f"Up to date at block {latest_block}")
                
                # Wait before checking for new blocks
                print(f"Waiting {sleep_seconds}s before checking for new blocks...")
                time.sleep(sleep_seconds)
                    
        except KeyboardInterrupt:
            print("\nStopping monitor...")
            self.save_data()
            self.print_summary()

def main():
    # Configuration - get from environment or use defaults
    RPC_URL = os.environ.get('RPC_URL')
    if not RPC_URL:
        RPC_URL = "https://base.llamarpc.com
        print(f"No RPC_URL environment variable found, using default RPC endpoint")
    else:
        print(f"Using RPC_URL from environment variable")
    
    START_BLOCK = 35956776
    SAVE_FILE = "uniswap_v4_data_testnet.json"
    
    print(f"Connecting to RPC: {RPC_URL[:50]}...")
    
    # Test connection first
    try:
        web3 = Web3(Web3.HTTPProvider(RPC_URL))
        latest_block = web3.eth.get_block('latest')
        print(f"Successfully connected to RPC. Latest block: {latest_block['number']}")
    except Exception as e:
        print(f"Failed to connect to RPC: {e}")
        exit(1)
    
    # Create the monitor
    monitor = UniswapV4Monitor(RPC_URL, START_BLOCK, SAVE_FILE)
    
    # For GitHub Actions, run once with a reasonable block limit
    print("Running single scan for GitHub Actions...")
    try:
        # Scan up to 2000 blocks in one run to avoid timeout
        monitor.run_once(blocks_per_scan=2000)
        print("GitHub Actions scan completed successfully!")
    except Exception as e:
        print(f"Error during GitHub Actions scan: {e}")
        # Save whatever progress was made
        monitor.save_data()
        exit(1)

if __name__ == "__main__":
    main()
